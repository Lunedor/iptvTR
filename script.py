import asyncio
import logging
import aiohttp
import re
from dataclasses import dataclass, field
from typing import List, Dict
import difflib
import urllib.parse

# Ayarları yapılandır
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Zamanaşımı süresi
TIMEOUT = 5

# M3U kaynak linkleri (Buraya güncel linkleri ekleyebilirsin)
M3U_SOURCES = [
    'https://raw.githubusercontent.com/GitLatte/patr0n/refs/heads/site/lists/iptvsevenler.m3u',
    'https://raw.githubusercontent.com/keyiflerolsun/IPTV_YenirMi/refs/heads/main/Kanallar/KekikAkademi.m3u',
    'https://tinyurl.com/TVCANLI'
]

# Kategori eşleştirme (daha iyi eşleşme için kullanılıyor)
CATEGORY_MAPPING = {
    "Sport": "Sports",
    "Movie": "Movies",
    "News": "News & Politics"
}

# Filtrelenmesi gereken kategoriler
FILTER_CATEGORIES = ["Adult", "XXX", "Erotic"]


@dataclass
class Channel:
    """Kanal verilerini saklamak için dataclass."""
    name: str
    category: str
    url: str
    metadata: dict = field(default_factory=dict)  # Ek meta bilgileri için

    def __repr__(self):
        return f"Channel(name={self.name}, category={self.category}, url={self.url})"


# URL'nin çalışıp çalışmadığını asenkron kontrol et (Semaphore ile)
async def check_url(sem: asyncio.Semaphore, url: str) -> str | None:
    """Asenkron olarak URL'nin çalışıp çalışmadığını kontrol et, eşzamanlı bağlantıları sınırla."""
    async with sem:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=TIMEOUT) as response:
                    response.raise_for_status()
                    return url
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logging.warning(f"URL Kontrol Hatası: {url}, Hata: {e}")
            return None


async def remove_dead_links(channels: List[Channel]) -> List[Channel]:
    """Ölü linkleri temizle, eşzamanlı bağlantı sınırı koy."""
    sem = asyncio.Semaphore(10)  # Aynı anda en fazla 10 istek
    tasks = [check_url(sem, ch.url) for ch in channels]
    results = await asyncio.gather(*tasks)
    return [ch for ch, result in zip(channels, results) if result is not None]


# Kategori normalizasyonu (hem yazım hatası düzeltme hem de difflib ile eşleştirme)
def normalize_category(category: str, use_mapping: bool = False, use_difflib: bool = False) -> str:
    """Kategoriyi normalize et ve eşleştir. Hem yazım hatası düzeltme hem de difflib ile eşleştirme."""
    category = category.strip().title()
    category = " ".join(category.split())  # Çift boşlukları temizle

    if use_mapping:
        if use_difflib:
            match = difflib.get_close_matches(category, CATEGORY_MAPPING.keys(), n=1, cutoff=0.7)
            if match:
                return CATEGORY_MAPPING[match[0]]
        else:
            for key, value in CATEGORY_MAPPING.items():
                if key.lower() in category.lower():
                    return value
    return category


# Kanalları temizleme (Filtreleme ve Kategori Normalizasyonu)
def clean_channels(channels: List[Channel], use_mapping: bool = False, use_difflib: bool = False) -> List[Channel]:
    """Filtreleme ve kategori düzenleme işlemi."""
    cleaned = []
    for ch in channels:
        normalized_category = normalize_category(ch.category, use_mapping, use_difflib)

        # Genişletilmiş kategori filtreleme
        if any(f.lower() in normalized_category.lower() for f in FILTER_CATEGORIES):
            logging.info(f"Filtrelendi: {ch.name} - Kategori: {normalized_category}")
            continue

        ch.category = normalized_category
        cleaned.append(ch)
    return cleaned


# M3U Ayrıştırma (Boş kanal adlarını düzeltme)
def parse_m3u(m3u_content: str) -> List[Channel]:
    """M3U içeriğini ayrıştır, eksik kanal adlarını düzelt."""
    channels: List[Channel] = []
    current_channel: Dict = {}

    for line in m3u_content.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF"):
            # Daha esnek regex
            match = re.search(r'tvg-name="([^"]*)".*(?:tvg-group="([^"]*)"|group-title="([^"]*)").*', line)
            
            if match:
                name = match.group(1).strip() or "Unknown Channel"  # Varsayılan ad
                category = match.group(2) or match.group(3) or "Various" # tvg-group veya group-title al
                category = category.strip()

                current_channel["name"] = name
                current_channel["category"] = category

            else:
                 logging.warning(f"EXTINF bilgisi bulunamadı: {line}")

                 continue

        elif line and not line.startswith("#"):
             current_channel["url"] = line.strip()
             if "name" in current_channel and "category" in current_channel and "url" in current_channel:
                channels.append(Channel(**current_channel))
                current_channel = {}
             else:
                 logging.warning(f"Kanal bilgileri eksik: {line}")
    return channels


# Asenkron M3U indirme
async def download_m3u(url: str) -> str | None:
    """M3U listesini asenkron indir."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                response.raise_for_status()
                return await response.text()
    except aiohttp.ClientError as e:
        logging.error(f"M3U indirilemedi: {url}, Hata: {e}")
        return None


def generate_m3u(channels: List[Channel]) -> str:
    """Temizlenmiş listeyi M3U formatına çevir."""
    m3u_content = "#EXTM3U\n"
    for ch in channels:
        m3u_content += f'#EXTINF:-1 tvg-name="{ch.name}" tvg-group="{ch.category}",{ch.name}\n'
        m3u_content += f'{ch.url}\n'
    return m3u_content

# Asenkron M3U indirme ve işleme (Birden fazla M3U kaynağı için)
async def process_m3u_sources(m3u_sources: List[str]) -> List[Channel]:
    """M3U kaynaklarını indirip işler."""
    all_channels = []
    for url in m3u_sources:
        m3u_content = await download_m3u(url)
        if m3u_content:
            channels = parse_m3u(m3u_content)
            all_channels.extend(channels)  # Kanalları topluyoruz

    return all_channels


# Örnek Kullanım
async def main():
    # M3U kaynaklarını işle
    all_channels = await process_m3u_sources(M3U_SOURCES)
    logging.info(f"Toplam Kanal Sayısı (işlenmeden önce): {len(all_channels)}")

    # Ölü linkleri temizleme
    channels = await remove_dead_links(all_channels)
    logging.info(f"Geçerli Kanal Sayısı (ölü linkler temizlendikten sonra): {len(channels)}")

    # Kategorileri düzenleme ve filtreleme
    # Varsayılan olarak sadece yazım hatası düzeltme
    cleaned_channels = clean_channels(channels)
    logging.info(f"Temizlenen Kanal Sayısı (varsayılan): {len(cleaned_channels)}")

    # Manuel mapping yapma seçeneği (use_difflib = False varsayılan)
    cleaned_channels = clean_channels(channels, use_mapping=True)
    logging.info(f"Temizlenen Kanal Sayısı (manual mapping): {len(cleaned_channels)}")

    # difflib ile mapping yapma seçeneği
    cleaned_channels = clean_channels(channels, use_mapping=True, use_difflib=True)
    logging.info(f"Temizlenen Kanal Sayısı (difflib mapping): {len(cleaned_channels)}")

    # Sonuçları göster
    for ch in cleaned_channels:
        logging.info(ch)

    final_m3u = generate_m3u(cleaned_channels)

    with open("filtered_playlist.m3u", "w", encoding="utf-8") as f:
        f.write(final_m3u)

    logging.info("Yeni M3U listesi oluşturuldu: filtered_playlist.m3u")

# Asenkron çalıştır
if __name__ == "__main__":
    asyncio.run(main())