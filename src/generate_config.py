import csv
import re
import os

def generate_config():
    input_file = r"C:\Users\highk\.gemini\antigravity\brain\0bfcd7a0-000a-4eec-ba0a-b38fb2ec73f9\.system_generated\steps\32\content.md"
    output_file = r"d:\Projects\BDATLProject\src\config_tickers.py"

    with open(input_file, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Extract CSV lines (starts from Company Name)
    match = re.search(r"(Company Name,Industry,Symbol,Series,ISIN Code.*)", content, flags=re.DOTALL)
    if not match:
        print("CSV header not found!")
        return
    
    csv_text = match.group(1).strip()
    reader = csv.DictReader(csv_text.splitlines())
    
    tickers = []
    sectors = {}
    names = {}
    keywords = {}
    
    for row in reader:
        symbol = row.get("Symbol", "").strip()
        if not symbol:
            continue
            
        company_name = row.get("Company Name", "").strip()
        industry = row.get("Industry", "").strip()
        
        tickers.append(symbol)
        sectors[symbol] = industry
        names[symbol] = company_name
        
        # generate a few keywords based on the first word of the company name usually, plus symbol
        kw = set()
        kw.add(symbol.lower())
        
        name_clean = re.sub(r'(?i)\b(ltd\.|limited|co\.|corporation|company)\b', '', company_name).strip()
        first_word = name_clean.split()[0].lower() if name_clean else ""
        if len(first_word) > 3:
            kw.add(first_word)
        
        keywords[symbol] = list(kw)

    # Now write to python file
    with open(output_file, "w", encoding="utf-8") as out:
        out.write('"""\nAuto-generated Nifty 500 configuration for BDATL pipeline.\n"""\n\n')
        out.write("NIFTY_500_TICKERS = [\n")
        for i in range(0, len(tickers), 10):
            batch = tickers[i:i+10]
            out.write("    " + ", ".join(f'"{t}"' for t in batch) + ",\n")
        out.write("]\n\n")
        
        out.write("NIFTY_500_SECTORS = {\n")
        for sym, ind in sectors.items():
            out.write(f'    "{sym}": "{ind}",\n')
        out.write("}\n\n")

        out.write("NIFTY_500_NAMES = {\n")
        for sym, nam in names.items():
            # escape quotes
            nam_escaped = nam.replace('"', '\\"')
            out.write(f'    "{sym}": "{nam_escaped}",\n')
        out.write("}\n\n")

        out.write("NIFTY_500_KEYWORDS = {\n")
        for sym, kws in keywords.items():
            out.write(f'    "{sym}": {kws},\n')
        out.write("}\n\n")
        
    print(f"Generated {output_file} with {len(tickers)} tickers.")

if __name__ == "__main__":
    generate_config()
