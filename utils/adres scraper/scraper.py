from scraper import RegonScraper

with RegonScraper(headless=False) as scraper:
    wynik = scraper.scrape_nip("5273012424")
    print(wynik)
