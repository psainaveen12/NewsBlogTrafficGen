from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class UserAgentProfile:
    name: str
    weight: int
    user_agent: str


TOP_25_USER_AGENTS: list[UserAgentProfile] = [
    UserAgentProfile(
        name="chrome_windows_134",
        weight=11,
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    ),
    UserAgentProfile(
        name="chrome_windows_133",
        weight=8,
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    ),
    UserAgentProfile(
        name="edge_windows_134",
        weight=8,
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
    ),
    UserAgentProfile(
        name="edge_windows_133",
        weight=5,
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
    ),
    UserAgentProfile(
        name="chrome_macos_134",
        weight=7,
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    ),
    UserAgentProfile(
        name="chrome_macos_133",
        weight=4,
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    ),
    UserAgentProfile(
        name="safari_macos_18_4",
        weight=7,
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15",
    ),
    UserAgentProfile(
        name="firefox_windows_136",
        weight=4,
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0",
    ),
    UserAgentProfile(
        name="firefox_macos_136",
        weight=3,
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:136.0) Gecko/20100101 Firefox/136.0",
    ),
    UserAgentProfile(
        name="chrome_android_134_pixel8",
        weight=10,
        user_agent="Mozilla/5.0 (Linux; Android 15; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Mobile Safari/537.36",
    ),
    UserAgentProfile(
        name="chrome_android_133_pixel7",
        weight=6,
        user_agent="Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Mobile Safari/537.36",
    ),
    UserAgentProfile(
        name="samsung_android_24",
        weight=4,
        user_agent="Mozilla/5.0 (Linux; Android 14; SAMSUNG SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/24.0 Chrome/120.0.0.0 Mobile Safari/537.36",
    ),
    UserAgentProfile(
        name="samsung_android_23",
        weight=3,
        user_agent="Mozilla/5.0 (Linux; Android 14; SAMSUNG SM-A546E) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/23.0 Chrome/120.0.0.0 Mobile Safari/537.36",
    ),
    UserAgentProfile(
        name="safari_ios_18_4_iphone15",
        weight=8,
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 18_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Mobile/15E148 Safari/604.1",
    ),
    UserAgentProfile(
        name="safari_ios_18_3_iphone14",
        weight=6,
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 18_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Mobile/15E148 Safari/604.1",
    ),
    UserAgentProfile(
        name="chrome_ios_134_iphone15",
        weight=4,
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 18_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/134.0.0.0 Mobile/15E148 Safari/604.1",
    ),
    UserAgentProfile(
        name="chrome_ios_133_iphone14",
        weight=3,
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 18_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/133.0.0.0 Mobile/15E148 Safari/604.1",
    ),
    UserAgentProfile(
        name="firefox_ios_136",
        weight=2,
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 18_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/136.0 Mobile/15E148 Safari/605.1.15",
    ),
    UserAgentProfile(
        name="edge_ios_134",
        weight=2,
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 18_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) EdgiOS/134.0.0 Mobile/15E148 Safari/605.1.15",
    ),
    UserAgentProfile(
        name="chrome_linux_134",
        weight=3,
        user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    ),
    UserAgentProfile(
        name="firefox_linux_136",
        weight=2,
        user_agent="Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0",
    ),
    UserAgentProfile(
        name="safari_ipad_18_4",
        weight=3,
        user_agent="Mozilla/5.0 (iPad; CPU OS 18_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Mobile/15E148 Safari/604.1",
    ),
    UserAgentProfile(
        name="chrome_android_tablet_134",
        weight=2,
        user_agent="Mozilla/5.0 (Linux; Android 14; SM-X910) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    ),
    UserAgentProfile(
        name="opera_windows_117",
        weight=1,
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 OPR/117.0.0.0",
    ),
    UserAgentProfile(
        name="opera_android_84",
        weight=1,
        user_agent="Mozilla/5.0 (Linux; Android 14; CPH2573) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36 OPR/84.0.2254.73871",
    ),
]


def top_25_traffic_profiles_dicts() -> list[dict]:
    return [asdict(profile) for profile in TOP_25_USER_AGENTS]

