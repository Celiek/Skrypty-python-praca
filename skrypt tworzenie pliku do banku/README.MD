# CLI do generowania faktur łączonych do mbanku

## Opis:
Program cli tworzy plik zgodny ze standardem Elixir-0: [bresok mbank](https://www.mbank.pl/pdf/msp-korporacje/mbank-companynet/mbank-companynet-bresok-struktura-zbioru-importu-eksportu-danych-w-formacie-elixir-v1.81.pdf),
program na wejsciu pobiera plik *.xlsx plik wymaga tych kolumn :![img_2.png](img_2.png).
Po wygenerowaniu danych program tworzy pliki raportów wraz z ilością scalonych faktur
kod działa w trybie cli z po

### Szybki opis uruchomienia programu 
Progra uruchamia się komendą :
`py .\elixr_bo_banku.py "plik.xlsx" -c "nazwa_firmy`

Dodatkowe opcje i opis działania:
* -c shumme/greatstore/extrastore - generuje odpowiednik plik z wybraną firmą
* -o "plik.txt" - eksportuje plik pod wybraną nazwą
* --dup error - blokuje duplikaty w pliku
* --no-headless - włącza widok selenium ( do debugowania)
* --per-group-dir 'nazwa_folderu' - zapisuje listę plików do zdefiniowanego folderu

## Sposób sprawdzania danych:
Program odzcytuje dane z pliku xlsx (wprowadzonego jako "przełącznik") następnie
sprawdza czy istnieją takie kolumny: ![img_2.png](img_2.png), jeśli nie istnieją przerwywa działanie.
W przeciwnym wypadku przechodzi do odczytywania danych i sprawdzania ich poprawności min.:
* zmienia format daty - z DDMMYYYY ma YYYYMMDD
* usuwa nieporządane znaki z nazw ( znaki : *;!+?# )
* sprawdza czy nip jest w poprawnym formacie i czy istnieje w bazie gusu i lokalnej bazie danych, jeśli nie program kończy działanie i pokazuje błąd
* nr konta bankowego jest zaczytywany z bazy danych, jeśli nie istnieje w bazie danych nr konta zostanie zamieniony na 26 zer
* sprawdza adres firmy z lokalnej bazy danych ( jeśli nie ma go w bazie adres jest pobierany z danych gusu( po nipie ) i zapisywany w bazie lokalnej).

## Zapis do pliku:
Program po sprawdzeniu i zweryfikowaniu danych, zapisuje wynik do pliku .txt,
jesli w pliku znajdują się duble to zostaną usunięte a nazwa faktury nazwa kontrahenta i nip zostanie zapisany w 
raporcie, lista faktur od danego kontrahenta z danego dnia zostaje zapisana w plikach lokalnie po dacie faktury w folderze.
Dodatkowo całkowity raport jest generowany w formacie xlsx do szybkiego sprawdzenia.

