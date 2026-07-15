"""
job_scraper.py
================
사람인 / 링커리어 / 캐치 3개 채용 사이트를 대상으로 하는 멀티 소스 스크래핑 에이전트.

핵심 동작
---------
1) 각 사이트의 "최신순" 목록 페이지를 페이지네이션하며 공고를 수집한다.
2) 이미 수집한 공고(직전 실행 결과 또는 현재 실행 중 이미 본 공고)와의 "중복 비율"이
   임계치를 넘으면 그 사이트 크롤링을 중단한다.  최신순 정렬이므로, 중복 구간에
   도달했다는 것은 그 뒤로는 전부 과거에 수집한 오래된 공고라는 의미다.
3) 신규 공고는 JSONL 로 append 저장하고, 중복 판별용 seen-id 집합은 JSON 으로 영속화한다.

설계 포인트
-----------
- Fetcher(수집)와 Parser(파싱)를 분리 → 사이트별로 최적 수집기를 선택.
  · 사람인/링커리어 : SSR(HTML 에 목록이 들어있음) → httpx 로 충분(빠름).
  · 캐치            : 봇 탐지 존재 → Playwright 헤드리스 브라우저로 우회.
- 파싱은 CSS 클래스명(자주 바뀜) 대신 "상세페이지 URL 의 고유 ID 패턴"에 의존하여
  마크업 변경에 강하게 만든다.  부가 메타데이터(회사/지역/마감)는 best-effort.
- 광고/고정(pinned) 공고가 매 페이지 상단에 반복 노출되어도 조기 종료되지 않도록,
  단건 중복이 아니라 "페이지 내 신규 비율"을 종료 기준으로 사용한다.

주의(법적/운영)
---------------
- 실제 운영 전 각 사이트의 robots.txt 및 이용약관을 반드시 확인할 것.
- 사람인은 공식 오픈 API(oapi.saramin.co.kr)를 제공한다. 대량/상용 목적이라면
  스크래핑보다 공식 API 사용을 권장한다(파일 하단 참고 함수).
- 서버 부하를 주지 않도록 요청 간 지연(delay)과 페이지 상한(max_pages)을 준수한다.

필요 패키지:  pip install httpx beautifulsoup4 lxml playwright
Playwright 최초 1회:  playwright install chromium
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import time
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional, Protocol

import httpx
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------- #
# 로깅
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("job_scraper")


# --------------------------------------------------------------------------- #
# 설정
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ScrapeConfig:
    """크롤링 전반의 파라미터."""
    max_pages: int = 20               # 사이트당 페이지 상한(안전장치 / 최초 실행 시 무한루프 방지)
    min_new_ratio: float = 0.2        # 페이지 내 신규 비율이 이 값 미만이면 중복 구간 → 종료
    delay_range: tuple[float, float] = (1.5, 3.5)   # 요청 간 지연(초) 랜덤 범위(예의상)
    request_timeout: float = 15.0     # HTTP 타임아웃(초)
    max_retries: int = 3              # 요청 실패 시 재시도 횟수

    # 브라우저처럼 보이게 하는 기본 헤더(SSR 사이트용)
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )


# --------------------------------------------------------------------------- #
# 데이터 모델
# --------------------------------------------------------------------------- #
@dataclass
class JobPosting:
    """수집된 채용공고 1건."""
    source: str                       # "saramin" | "linkareer" | "catch"
    job_id: str                       # 사이트 내부 고유 ID(rec_idx / activity id / detail id)
    title: str
    detail_url: str
    company: Optional[str] = None
    location: Optional[str] = None
    employment_type: Optional[str] = None
    deadline: Optional[str] = None
    scraped_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def uid(self) -> str:
        """사이트를 가로지르는 전역 고유 키(중복 판별용)."""
        return f"{self.source}:{self.job_id}"


# --------------------------------------------------------------------------- #
# 중복 판별용 영속 저장소
# --------------------------------------------------------------------------- #
class SeenStore:
    """이미 수집한 공고 uid 집합을 JSON 파일로 로드/저장한다."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._seen: set[str] = set()
        if self.path.exists():
            try:
                self._seen = set(json.loads(self.path.read_text(encoding="utf-8")))
                log.info("seen-store 로드: %d건", len(self._seen))
            except (json.JSONDecodeError, OSError) as exc:
                log.warning("seen-store 로드 실패(%s) → 빈 집합으로 시작", exc)

    def __contains__(self, uid: str) -> bool:
        return uid in self._seen

    def add(self, uid: str) -> None:
        self._seen.add(uid)

    def save(self) -> None:
        self.path.write_text(
            json.dumps(sorted(self._seen), ensure_ascii=False, indent=0),
            encoding="utf-8",
        )
        log.info("seen-store 저장: %d건 → %s", len(self._seen), self.path)


# --------------------------------------------------------------------------- #
# 수집기(Fetcher) : URL → HTML
# --------------------------------------------------------------------------- #
class Fetcher(Protocol):
    """수집기 인터페이스. fetch()는 렌더링된 HTML 문자열을 반환한다."""
    def fetch(self, url: str) -> str: ...
    def close(self) -> None: ...


def _retry(times: int) -> Callable:
    """간단한 재시도 데코레이터(지수 백오프)."""
    def deco(fn: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            last_exc: Optional[Exception] = None
            for attempt in range(1, times + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001 - 네트워크 계열 예외 광범위 캐치
                    last_exc = exc
                    wait = min(2 ** attempt, 10)
                    log.warning("요청 실패(%d/%d): %s → %.0fs 후 재시도", attempt, times, exc, wait)
                    time.sleep(wait)
            assert last_exc is not None
            raise last_exc
        return wrapper
    return deco


class HttpxFetcher:
    """SSR 사이트(사람인/링커리어)용 경량 HTTP 수집기."""

    def __init__(self, config: ScrapeConfig):
        self._client = httpx.Client(
            headers={
                "User-Agent": config.user_agent,
                "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=config.request_timeout,
            follow_redirects=True,
        )
        self._retries = config.max_retries

    def fetch(self, url: str) -> str:
        @_retry(self._retries)
        def _do() -> str:
            resp = self._client.get(url)
            resp.raise_for_status()
            return resp.text
        return _do()

    def close(self) -> None:
        self._client.close()


class PlaywrightFetcher:
    """
    봇 탐지가 있는 사이트(캐치)용 헤드리스 브라우저 수집기.
    브라우저를 1회만 띄우고 컨텍스트를 재사용하여 페이지 간 오버헤드를 줄인다.
    (Playwright 는 지연 임포트 → 미설치 환경에서도 모듈 임포트 자체는 성공)
    """

    def __init__(self, config: ScrapeConfig, wait_selector: Optional[str] = None):
        from playwright.sync_api import sync_playwright  # 지연 임포트

        self._config = config
        self._wait_selector = wait_selector
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=True)
        self._context = self._browser.new_context(
            user_agent=config.user_agent,
            locale="ko-KR",
            viewport={"width": 1366, "height": 900},
        )
        self._page = self._context.new_page()

    def fetch(self, url: str) -> str:
        @_retry(self._config.max_retries)
        def _do() -> str:
            self._page.goto(url, wait_until="networkidle",
                            timeout=int(self._config.request_timeout * 1000))
            if self._wait_selector:
                # 목록 컨테이너가 렌더링될 때까지 대기(선택자는 사이트에 맞게 조정)
                try:
                    self._page.wait_for_selector(self._wait_selector, timeout=8000)
                except Exception:  # noqa: BLE001 - 선택자 미존재 시에도 현재 DOM 반환
                    pass
            return self._page.content()
        return _do()

    def close(self) -> None:
        self._context.close()
        self._browser.close()
        self._pw.stop()


# --------------------------------------------------------------------------- #
# 파싱 유틸 : 상세 URL 의 고유 ID 패턴 기반(마크업 변경에 강함)
# --------------------------------------------------------------------------- #
def _soup(html: str) -> BeautifulSoup:
    # lxml 이 있으면 빠르게, 없으면 표준 파서로 폴백
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:  # noqa: BLE001
        return BeautifulSoup(html, "html.parser")


def extract_by_id_pattern(
    html: str,
    *,
    source: str,
    id_regex: re.Pattern[str],
    detail_url_builder: Callable[[str], str],
    enrich: Optional[Callable[[object, str], dict]] = None,
) -> list[JobPosting]:
    """
    href 가 `id_regex` 에 매칭되는 모든 앵커에서 공고를 추출한다.

    - 같은 공고가 (로고 링크 + 제목 링크)처럼 여러 앵커로 중복 등장할 수 있으므로
      페이지 내에서 job_id 기준으로 1건만 남긴다(제목이 있는 앵커를 우선).
    - enrich(row_element, job_id) 가 주어지면 회사/지역/마감 등 부가정보를 채운다.
    """
    soup = _soup(html)
    picked: dict[str, JobPosting] = {}

    for a in soup.find_all("a", href=True):
        m = id_regex.search(a["href"])
        if not m:
            continue
        job_id = m.group(1)
        title = a.get_text(strip=True)

        # 이미 담긴 공고인데 기존 제목이 비어있지 않다면 스킵(더 나은 제목 유지)
        if job_id in picked and picked[job_id].title:
            continue

        posting = JobPosting(
            source=source,
            job_id=job_id,
            title=title,
            detail_url=detail_url_builder(job_id),
        )

        # 부가 메타데이터(best-effort): 앵커의 상위 행/아이템 컨테이너에서 추출
        if enrich is not None:
            row = a.find_parent(["tr", "li", "article", "div"])
            if row is not None:
                try:
                    for k, v in enrich(row, job_id).items():
                        setattr(posting, k, v)
                except Exception:  # noqa: BLE001 - 부가정보 실패는 무시(핵심 필드는 확보됨)
                    pass

        picked[job_id] = posting

    return list(picked.values())


# --------------------------------------------------------------------------- #
# 사이트 스크래퍼 베이스 : 공통 "중복까지 수집" 루프
# --------------------------------------------------------------------------- #
class BaseScraper(ABC):
    source: str = "base"

    def __init__(self, fetcher: Fetcher, config: ScrapeConfig):
        self.fetcher = fetcher
        self.config = config

    @abstractmethod
    def page_url(self, page: int) -> str:
        """페이지 번호 → 목록 URL. 사이트별 페이지네이션 규칙 구현."""

    @abstractmethod
    def parse(self, html: str) -> list[JobPosting]:
        """HTML → 공고 리스트. 사이트별 파싱 구현."""

    def scrape(self, seen: SeenStore) -> list[JobPosting]:
        """
        최신순 페이지를 순회하며 신규 공고를 모은다.
        종료 조건:
          (a) 페이지 신규 비율 < min_new_ratio  → 중복 구간 도달
          (b) 파싱 결과 0건                     → 마지막 페이지 도달
          (c) page > max_pages                  → 안전 상한
        """
        collected: list[JobPosting] = []
        run_ids: set[str] = set()   # 현재 실행에서 이미 담은 uid(페이지 간 중복 방지)

        for page in range(1, self.config.max_pages + 1):
            url = self.page_url(page)
            log.info("[%s] page %d 수집: %s", self.source, page, url)

            html = self.fetcher.fetch(url)
            postings = self.parse(html)

            if not postings:
                log.info("[%s] page %d 결과 없음 → 종료", self.source, page)
                break

            new_on_page = 0
            for p in postings:
                if p.uid in seen or p.uid in run_ids:
                    continue  # 과거 실행 or 이번 실행에서 이미 본 공고 → 중복
                run_ids.add(p.uid)
                collected.append(p)
                new_on_page += 1

            ratio = new_on_page / len(postings)
            log.info("[%s] page %d: 신규 %d/%d (비율 %.0f%%)",
                     self.source, page, new_on_page, len(postings), ratio * 100)

            # 중복 구간 도달 판정(광고/고정 공고로 인한 조기 종료 방지를 위해 '비율' 사용)
            if ratio < self.config.min_new_ratio:
                log.info("[%s] 신규 비율이 임계치 미만 → 중복 구간 도달, 종료", self.source)
                break

            self._polite_sleep()

        log.info("[%s] 수집 완료: 신규 %d건", self.source, len(collected))
        return collected

    def _polite_sleep(self) -> None:
        lo, hi = self.config.delay_range
        time.sleep(random.uniform(lo, hi))


# --------------------------------------------------------------------------- #
# 사람인
# --------------------------------------------------------------------------- #
class SaraminScraper(BaseScraper):
    source = "saramin"
    BASE = "https://www.saramin.co.kr/zf_user/jobs/public/list"
    # 상세 URL: /zf_user/jobs/relay/view?...&rec_idx=54162332
    ID_RE = re.compile(r"rec_idx=(\d+)")

    def page_url(self, page: int) -> str:
        # ⚠️ 검증필요: 실시간 공고 목록의 페이지 파라미터. 최신순 정렬 확인 권장.
        #    (사람인은 목록을 AJAX 로 갱신하므로, httpx 로 안 되면 PlaywrightFetcher 로 교체)
        return f"{self.BASE}?page={page}&sort_type=latest"

    def parse(self, html: str) -> list[JobPosting]:
        return extract_by_id_pattern(
            html,
            source=self.source,
            id_regex=self.ID_RE,
            detail_url_builder=lambda i: (
                f"https://www.saramin.co.kr/zf_user/jobs/relay/view"
                f"?rec_idx={i}&view_type=public-recruit"
            ),
            enrich=self._enrich,
        )

    @staticmethod
    def _enrich(row, job_id: str) -> dict:
        # best-effort: 아이템 컨테이너 내 회사명/지역 링크 텍스트를 추출
        # (클래스명은 자주 바뀌므로 구조·href 기반으로 방어적으로 접근)
        data: dict = {}
        company_a = row.find("a", href=re.compile(r"company-info"))
        if company_a:
            data["company"] = company_a.get_text(strip=True) or None
        return data


# --------------------------------------------------------------------------- #
# 링커리어
# --------------------------------------------------------------------------- #
class LinkareerScraper(BaseScraper):
    source = "linkareer"
    BASE = "https://linkareer.com/list/recruit"
    # 상세 URL: https://linkareer.com/activity/321489
    ID_RE = re.compile(r"/activity/(\d+)")

    def page_url(self, page: int) -> str:
        # 검증됨: RECENT DESC(최신순) + page 파라미터
        return (
            f"{self.BASE}?filterBy_activityTypeID=5&filterBy_status=OPEN"
            f"&orderBy_direction=DESC&orderBy_field=RECENT&page={page}"
        )

    def parse(self, html: str) -> list[JobPosting]:
        return extract_by_id_pattern(
            html,
            source=self.source,
            id_regex=self.ID_RE,
            detail_url_builder=lambda i: f"https://linkareer.com/activity/{i}",
            enrich=self._enrich,
        )

    @staticmethod
    def _enrich(row, job_id: str) -> dict:
        # 링커리어 목록은 테이블(tr) 구조 → 셀 텍스트에서 부가정보 best-effort 추출
        data: dict = {}
        cells = [c.get_text(strip=True) for c in row.find_all("td")]
        # 채용형태/지역/마감이 후반 셀에 위치(레이아웃 변경 시 인덱스 조정)
        for text in cells:
            if text in ("인턴", "신입", "경력직", "계약직", "신입, 경력직", "정규직"):
                data.setdefault("employment_type", text)
            elif text.startswith("~") or "마감" in text or "상시" in text:
                data.setdefault("deadline", text)
        return data


# --------------------------------------------------------------------------- #
# 캐치 (봇 탐지 → Playwright)
# --------------------------------------------------------------------------- #
class CatchScraper(BaseScraper):
    source = "catch"
    BASE = "https://www.catch.co.kr/NCS/RecruitSearch"
    # 상세 URL: https://www.catch.co.kr/NCS/RecruitInfoDetails/533114
    ID_RE = re.compile(r"/NCS/RecruitInfoDetails/(\d+)")
    # 목록 렌더링 대기용 선택자(공고 상세 링크가 나타날 때까지 대기)
    WAIT_SELECTOR = "a[href*='RecruitInfoDetails']"

    def page_url(self, page: int) -> str:
        # ⚠️ 검증필요: 캐치의 페이지 파라미터명. 최신순 정렬 파라미터도 함께 확인.
        return f"{self.BASE}?page={page}"

    def parse(self, html: str) -> list[JobPosting]:
        return extract_by_id_pattern(
            html,
            source=self.source,
            id_regex=self.ID_RE,
            detail_url_builder=lambda i: (
                f"https://www.catch.co.kr/NCS/RecruitInfoDetails/{i}"
            ),
        )


# --------------------------------------------------------------------------- #
# 결과 저장
# --------------------------------------------------------------------------- #
def append_jsonl(postings: Iterable[JobPosting], path: str | Path) -> int:
    """신규 공고를 JSONL 로 append. 저장 건수를 반환."""
    path = Path(path)
    n = 0
    with path.open("a", encoding="utf-8") as f:
        for p in postings:
            f.write(json.dumps(asdict(p), ensure_ascii=False) + "\n")
            n += 1
    return n


# --------------------------------------------------------------------------- #
# 오케스트레이터
# --------------------------------------------------------------------------- #
def run(output_jsonl: str = "jobs.jsonl", seen_path: str = "seen_ids.json") -> None:
    config = ScrapeConfig()
    seen = SeenStore(seen_path)

    # SSR 사이트는 httpx(빠름), 봇 탐지 사이트는 Playwright.
    http_fetcher = HttpxFetcher(config)
    scrapers: list[BaseScraper] = [
        SaraminScraper(http_fetcher, config),
        LinkareerScraper(http_fetcher, config),
    ]

    # 캐치는 Playwright 가 설치된 경우에만 활성화(미설치 시 건너뜀)
    catch_fetcher: Optional[PlaywrightFetcher] = None
    try:
        catch_fetcher = PlaywrightFetcher(config, wait_selector=CatchScraper.WAIT_SELECTOR)
        scrapers.append(CatchScraper(catch_fetcher, config))
    except Exception as exc:  # noqa: BLE001
        log.warning("캐치(Playwright) 비활성화: %s "
                    "(설치: pip install playwright && playwright install chromium)", exc)

    total_new = 0
    try:
        for scraper in scrapers:
            new_postings = scraper.scrape(seen)
            # 저장 + seen 갱신
            total_new += append_jsonl(new_postings, output_jsonl)
            for p in new_postings:
                seen.add(p.uid)
    finally:
        http_fetcher.close()
        if catch_fetcher is not None:
            catch_fetcher.close()
        seen.save()

    log.info("=== 전체 신규 수집: %d건 → %s ===", total_new, output_jsonl)


# --------------------------------------------------------------------------- #
# (참고) 사람인 공식 오픈 API 방식 — 상용/대량이면 스크래핑보다 이 방식을 권장
# --------------------------------------------------------------------------- #
def fetch_saramin_via_official_api(keyword: str = "", start: int = 0, count: int = 100) -> dict:
    """
    사람인 채용정보 오픈 API 예시.  API 키는 코드에 하드코딩하지 말고 환경변수로 주입한다.
      export SARAMIN_API_KEY=발급키
    """
    api_key = os.getenv("SARAMIN_API_KEY", "[YOUR_SARAMIN_API_KEY]")
    resp = httpx.get(
        "https://oapi.saramin.co.kr/job-search",
        params={
            "access-key": api_key,
            "keywords": keyword,
            "start": start,     # 페이지 시작 인덱스
            "count": count,     # 한 번에 가져올 건수(최대 110)
            "sort": "pd",       # pd=등록일 최신순
        },
        headers={"Accept": "application/json"},
        timeout=15.0,
    )
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    run()
