import os

data = """
360ONE|Financial Services
3MINDIA|Diversified
ABB|Capital Goods
ACC|Construction Materials
AIAENG|Capital Goods
APLAPOLLO|Capital Goods
AUBANK|Financial Services
AADHARHFC|Financial Services
AARTIIND|Chemicals
AAVAS|Financial Services
ABBOTINDIA|Healthcare
ACE|Capital Goods
ADANIENSOL|Power
ADANIENT|Metals & Mining
ADANIGREEN|Power
ADANIPORTS|Services
ADANIPOWER|Power
ATGL|Oil Gas & Consumable Fuels
AWL|Fast Moving Consumer Goods
ABCAPITAL|Financial Services
ABFRL|Consumer Services
ABREL|Forest Materials
ABSLAMC|Financial Services
AEGISLOG|Oil Gas & Consumable Fuels
AFFLE|Information Technology
AJANTPHARM|Healthcare
AKUMS|Healthcare
APLLTD|Healthcare
ALKEM|Healthcare
ALKYLAMINE|Chemicals
ALOKINDS|Textiles
ARE&M|Automobile and Auto Components
AMBER|Consumer Durables
AMBUJACEM|Construction Materials
ANANDRATHI|Financial Services
ANANTRAJ|Realty
ANGELONE|Financial Services
APARINDS|Capital Goods
APOLLOHOSP|Healthcare
APOLLOTYRE|Automobile and Auto Components
APTUS|Financial Services
ACI|Chemicals
ASAHIINDIA|Automobile and Auto Components
ASHOKLEY|Capital Goods
ASIANPAINT|Consumer Durables
ASTERDM|Healthcare
ASTRAZEN|Healthcare
ASTRAL|Capital Goods
ATUL|Chemicals
AUROPHARMA|Healthcare
AVANTIFEED|Fast Moving Consumer Goods
DMART|Consumer Services
AXISBANK|Financial Services
BASF|Chemicals
BEML|Capital Goods
BLS|Consumer Services
BSE|Financial Services
BAJAJ-AUTO|Automobile and Auto Components
BAJFINANCE|Financial Services
BAJAJFINSV|Financial Services
BAJAJHLDNG|Financial Services
BALAMINES|Chemicals
BALKRISIND|Automobile and Auto Components
BALRAMCHIN|Fast Moving Consumer Goods
BANDHANBNK|Financial Services
BANKBARODA|Financial Services
BANKINDIA|Financial Services
MAHABANK|Financial Services
BATAINDIA|Consumer Durables
BAYERCROP|Chemicals
BERGEPAINT|Consumer Durables
BDL|Capital Goods
BEL|Capital Goods
BHARATFORG|Automobile and Auto Components
BHEL|Capital Goods
BPCL|Oil Gas & Consumable Fuels
BHARTIARTL|Telecommunication
BHARTIHEXA|Telecommunication
BIKAJI|Fast Moving Consumer Goods
BIOCON|Healthcare
BIRLACORPN|Construction Materials
BSOFT|Information Technology
BLUEDART|Services
BLUESTARCO|Consumer Durables
BBTC|Fast Moving Consumer Goods
BOSCHLTD|Automobile and Auto Components
BRIGADE|Realty
BRITANNIA|Fast Moving Consumer Goods
MAPMYINDIA|Information Technology
CCL|Fast Moving Consumer Goods
CESC|Power
CGPOWER|Capital Goods
CIEINDIA|Automobile and Auto Components
CRISIL|Financial Services
CAMPUS|Consumer Durables
CANFINHOME|Financial Services
CANBK|Financial Services
CAPLIPOINT|Healthcare
CGCL|Financial Services
CARBORUNIV|Capital Goods
CASTROLIND|Oil Gas & Consumable Fuels
CEATLTD|Automobile and Auto Components
CELLO|Consumer Durables
CENTRALBK|Financial Services
CDSL|Financial Services
CENTURYPLY|Consumer Durables
CERA|Consumer Durables
CHALET|Consumer Services
CHAMBLFERT|Chemicals
CHEMPLASTS|Chemicals
CHENNPETRO|Oil Gas & Consumable Fuels
CHOLAHLDNG|Financial Services
CHOLAFIN|Financial Services
CIPLA|Healthcare
CUB|Financial Services
CLEAN|Chemicals
COALINDIA|Oil Gas & Consumable Fuels
COCHINSHIP|Capital Goods
COFORGE|Information Technology
COLPAL|Fast Moving Consumer Goods
CAMS|Financial Services
CONCORDBIO|Healthcare
CONCOR|Services
COROMANDEL|Chemicals
CRAFTSMAN|Automobile and Auto Components
CREDITACC|Financial Services
CROMPTON|Consumer Durables
CUMMINSIND|Capital Goods
CYIENT|Information Technology
DLF|Realty
DOMS|Fast Moving Consumer Goods
DABUR|Fast Moving Consumer Goods
DALBHARAT|Construction Materials
DATAPATTNS|Capital Goods
DEEPAKFERT|Chemicals
DEEPAKNTR|Chemicals
DELHIVERY|Services
DEVYANI|Consumer Services
DIVISLAB|Healthcare
DIXON|Consumer Durables
LALPATHLAB|Healthcare
DRREDDY|Healthcare
EIDPARRY|Chemicals
EIHOTEL|Consumer Services
EASEMYTRIP|Consumer Services
EICHERMOT|Automobile and Auto Components
ELECON|Capital Goods
ELGIEQUIP|Capital Goods
EMAMILTD|Fast Moving Consumer Goods
EMCURE|Healthcare
ENDURANCE|Automobile and Auto Components
ENGINERSIN|Construction
EQUITASBNK|Financial Services
ERIS|Healthcare
ESCORTS|Capital Goods
EXIDEIND|Automobile and Auto Components
NYKAA|Consumer Services
FEDERALBNK|Financial Services
FACT|Chemicals
FINEORG|Chemicals
FINCABLES|Capital Goods
FINPIPE|Capital Goods
FSL|Services
FIVESTAR|Financial Services
FORTIS|Healthcare
GRINFRA|Construction
GAIL|Oil Gas & Consumable Fuels
GVT&D|Capital Goods
GMRINFRA|Services
GRSE|Capital Goods
GICRE|Financial Services
GILLETTE|Fast Moving Consumer Goods
GLAND|Healthcare
GLAXO|Healthcare
GLENMARK|Healthcare
MEDANTA|Healthcare
GODIGIT|Financial Services
GPIL|Capital Goods
GODFRYPHLP|Fast Moving Consumer Goods
GODREJAGRO|Fast Moving Consumer Goods
GODREJCP|Fast Moving Consumer Goods
GODREJIND|Diversified
GODREJPROP|Realty
GRANULES|Healthcare
GRAPHITE|Capital Goods
GRASIM|Construction Materials
GESHIP|Services
GRINDWELL|Capital Goods
GAEL|Fast Moving Consumer Goods
FLUOROCHEM|Chemicals
GUJGASLTD|Oil Gas & Consumable Fuels
GMDCLTD|Metals & Mining
GNFC|Chemicals
GPPL|Services
GSFC|Chemicals
GSPL|Oil Gas & Consumable Fuels
HEG|Capital Goods
HBLPOWER|Automobile and Auto Components
HCLTECH|Information Technology
HDFCAMC|Financial Services
HDFCBANK|Financial Services
HDFCLIFE|Financial Services
HFCL|Telecommunication
HAPPSTMNDS|Information Technology
HAVELLS|Consumer Durables
HEROMOTOCO|Automobile and Auto Components
HSCL|Chemicals
HINDALCO|Metals & Mining
HAL|Capital Goods
HINDCOPPER|Metals & Mining
HINDPETRO|Oil Gas & Consumable Fuels
HINDUNILVR|Fast Moving Consumer Goods
HINDZINC|Metals & Mining
POWERINDIA|Capital Goods
HOMEFIRST|Financial Services
HONASA|Fast Moving Consumer Goods
HONAUT|Capital Goods
HUDCO|Financial Services
ICICIBANK|Financial Services
ICICIGI|Financial Services
ICICIPRULI|Financial Services
ISEC|Financial Services
IDBI|Financial Services
IDFCFIRSTB|Financial Services
IFCI|Financial Services
IIFL|Financial Services
INOXINDIA|Capital Goods
IRB|Construction
IRCON|Construction
ITC|Fast Moving Consumer Goods
ITI|Telecommunication
INDGN|Healthcare
INDIACEM|Construction Materials
INDIAMART|Consumer Services
INDIANB|Financial Services
IEX|Financial Services
INDHOTEL|Consumer Services
IOC|Oil Gas & Consumable Fuels
IOB|Financial Services
IRCTC|Consumer Services
IRFC|Financial Services
IREDA|Financial Services
IGL|Oil Gas & Consumable Fuels
INDUSTOWER|Telecommunication
INDUSINDBK|Financial Services
NAUKRI|Consumer Services
INFY|Information Technology
INOXWIND|Capital Goods
INTELLECT|Information Technology
INDIGO|Services
IPCALAB|Healthcare
JBCHEPHARM|Healthcare
JKCEMENT|Construction Materials
JBMA|Automobile and Auto Components
JKLAKSHMI|Construction Materials
JKTYRE|Automobile and Auto Components
JMFINANCIL|Financial Services
JSWENERGY|Power
JSWINFRA|Services
JSWSTEEL|Metals & Mining
JPPOWER|Power
J&KBANK|Financial Services
JINDALSAW|Capital Goods
JSL|Metals & Mining
JINDALSTEL|Metals & Mining
JIOFIN|Financial Services
JUBLFOOD|Consumer Services
JUBLINGREA|Chemicals
JUBLPHARMA|Healthcare
JWL|Capital Goods
JUSTDIAL|Consumer Services
JYOTHYLAB|Fast Moving Consumer Goods
JYOTICNC|Capital Goods
KPRMILL|Textiles
KEI|Capital Goods
KNRCON|Construction
KPITTECH|Information Technology
KSB|Capital Goods
KAJARIACER|Consumer Durables
KPIL|Construction
KALYANKJIL|Consumer Durables
KANSAINER|Consumer Durables
KARURVYSYA|Financial Services
KAYNES|Capital Goods
KEC|Construction
KFINTECH|Financial Services
KIRLOSBROS|Capital Goods
KIRLOSENG|Capital Goods
KOTAKBANK|Financial Services
KIMS|Healthcare
LTF|Financial Services
LTTS|Information Technology
LICHSGFIN|Financial Services
LTIM|Information Technology
LT|Construction
LATENTVIEW|Information Technology
LAURUSLABS|Healthcare
LEMONTREE|Consumer Services
LICI|Financial Services
LINDEINDIA|Chemicals
LLOYDSME|Metals & Mining
LUPIN|Healthcare
MMTC|Services
MRF|Automobile and Auto Components
LODHA|Realty
MGL|Oil Gas & Consumable Fuels
MAHSEAMLES|Capital Goods
M&MFIN|Financial Services
M&M|Automobile and Auto Components
MAHLIFE|Realty
MANAPPURAM|Financial Services
MRPL|Oil Gas & Consumable Fuels
MANKIND|Healthcare
MARICO|Fast Moving Consumer Goods
MARUTI|Automobile and Auto Components
MASTEK|Information Technology
MFSL|Financial Services
MAXHEALTH|Healthcare
MAZDOCK|Capital Goods
METROBRAND|Consumer Durables
METROPOLIS|Healthcare
MINDACORP|Automobile and Auto Components
MSUMI|Automobile and Auto Components
MOTILALOFS|Financial Services
MPHASIS|Information Technology
MCX|Financial Services
MUTHOOTFIN|Financial Services
NATCOPHARM|Healthcare
NBCC|Construction
NCC|Construction
NHPC|Power
NLCINDIA|Power
NMDC|Metals & Mining
NSLNISP|Metals & Mining
NTPC|Power
NH|Healthcare
NATIONALUM|Metals & Mining
NAVINFLUOR|Chemicals
NESTLEIND|Fast Moving Consumer Goods
NETWEB|Information Technology
NETWORK18|Media Entertainment & Publication
NEWGEN|Information Technology
NAM-INDIA|Financial Services
NUVAMA|Financial Services
NUVOCO|Construction Materials
OBEROIRLTY|Realty
ONGC|Oil Gas & Consumable Fuels
OIL|Oil Gas & Consumable Fuels
OLECTRA|Automobile and Auto Components
PAYTM|Financial Services
OFSS|Information Technology
POLICYBZR|Financial Services
PCBL|Chemicals
PIIND|Chemicals
PNBHOUSING|Financial Services
PNCINFRA|Construction
PTCIL|Capital Goods
PVRINOX|Media Entertainment & Publication
PAGEIND|Textiles
PATANJALI|Fast Moving Consumer Goods
PERSISTENT|Information Technology
PETRONET|Oil Gas & Consumable Fuels
PFIZER|Healthcare
PHOENIXLTD|Realty
PIDILITIND|Chemicals
PEL|Financial Services
PPLPHARMA|Healthcare
POLYMED|Healthcare
POLYCAB|Capital Goods
POONAWALLA|Financial Services
PFC|Financial Services
POWERGRID|Power
PRAJIND|Capital Goods
PRESTIGE|Realty
PGHH|Fast Moving Consumer Goods
PNB|Financial Services
QUESS|Services
RRKABEL|Capital Goods
RBLBANK|Financial Services
RECLTD|Financial Services
RHIM|Capital Goods
RITES|Construction
RADICO|Fast Moving Consumer Goods
RVNL|Construction
RAILTEL|Telecommunication
RAINBOW|Healthcare
RAJESHEXPO|Consumer Durables
RKFORGE|Automobile and Auto Components
RCF|Chemicals
RATNAMANI|Capital Goods
RTNINDIA|Consumer Services
RAYMOND|Realty
REDINGTON|Services
RELIANCE|Oil Gas & Consumable Fuels
ROUTE|Telecommunication
SBFC|Financial Services
SBICARD|Financial Services
SBILIFE|Financial Services
SJVN|Power
SKFINDIA|Capital Goods
SRF|Chemicals
SAMMAANCAP|Financial Services
MOTHERSON|Automobile and Auto Components
SANOFI|Healthcare
SAPPHIRE|Consumer Services
SAREGAMA|Media Entertainment & Publication
SCHAEFFLER|Automobile and Auto Components
SCHNEIDER|Capital Goods
SCI|Services
SHREECEM|Construction Materials
RENUKA|Fast Moving Consumer Goods
SHRIRAMFIN|Financial Services
SHYAMMETL|Capital Goods
SIEMENS|Capital Goods
SIGNATURE|Realty
SOBHA|Realty
SOLARINDS|Chemicals
SONACOMS|Automobile and Auto Components
SONATSOFTW|Information Technology
STARHEALTH|Financial Services
SBIN|Financial Services
SAIL|Metals & Mining
SWSOLAR|Construction
SUMICHEM|Chemicals
SPARC|Healthcare
SUNPHARMA|Healthcare
SUNTV|Media Entertainment & Publication
SUNDARMFIN|Financial Services
SUNDRMFAST|Automobile and Auto Components
SUPREMEIND|Capital Goods
SUVENPHAR|Healthcare
SUZLON|Capital Goods
SWANENERGY|Diversified
SYNGENE|Healthcare
SYRMA|Capital Goods
TBOTEK|Consumer Services
TVSMOTOR|Automobile and Auto Components
TVSSCS|Services
TANLA|Information Technology
TATACHEM|Chemicals
TATACOMM|Telecommunication
TCS|Information Technology
TATACONSUM|Fast Moving Consumer Goods
TATAELXSI|Information Technology
TATAINVEST|Financial Services
TATAMOTORS|Automobile and Auto Components
TATAPOWER|Power
TATASTEEL|Metals & Mining
TATATECH|Information Technology
TTML|Telecommunication
TECHM|Information Technology
TECHNOE|Construction
TEJASNET|Telecommunication
NIACL|Financial Services
RAMCOCEM|Construction Materials
THERMAX|Capital Goods
TIMKEN|Capital Goods
TITAGARH|Capital Goods
TITAN|Consumer Durables
TORNTPHARM|Healthcare
TORNTPOWER|Power
TRENT|Consumer Services
TRIDENT|Textiles
TRIVENI|Fast Moving Consumer Goods
TRITURBINE|Capital Goods
TIINDIA|Automobile and Auto Components
UCOBANK|Financial Services
UNOMINDA|Automobile and Auto Components
UPL|Chemicals
UTIAMC|Financial Services
UJJIVANSFB|Financial Services
ULTRACEMCO|Construction Materials
UNIONBANK|Financial Services
UBL|Fast Moving Consumer Goods
UNITDSPR|Fast Moving Consumer Goods
USHAMART|Capital Goods
VGUARD|Consumer Durables
VIPIND|Consumer Durables
DBREALTY|Realty
VTL|Textiles
VARROC|Automobile and Auto Components
VBL|Fast Moving Consumer Goods
MANYAVAR|Consumer Services
VEDL|Metals & Mining
VIJAYA|Healthcare
VINATIORGA|Chemicals
IDEA|Telecommunication
VOLTAS|Consumer Durables
WELCORP|Capital Goods
WELSPUNLIV|Textiles
WESTLIFE|Consumer Services
WHIRLPOOL|Consumer Durables
WIPRO|Information Technology
YESBANK|Financial Services
ZFCVINDIA|Automobile and Auto Components
ZEEL|Media Entertainment & Publication
ZENSARTECH|Information Technology
ZOMATO|Consumer Services
ZYDUSLIFE|Healthcare
ECLERX|Services
"""

lines = [line.strip() for line in data.split("\\n") if line.strip()]

output = \"\"\"# src/config/tickers.py
# =====================
# Central mapping of NIFTY 500 Stocks to their respective Industries/Sectors.

TICKER_MAP = {
\"\"\"

for line in lines:
    parts = line.split('|')
    ticker_sym = parts[0] + '.NS'
    sector = parts[1].strip()
    output += f'    "{ticker_sym}": "{sector}",\\n'

output += \"\"\"}

# Ordered list of all NIFTY 500 tickers for ingestion
TICKER_LIST = sorted(list(TICKER_MAP.keys()))

# Dictionary mapping raw Yahoo symbols back to company keywords for NLP
# (We strip .NS for the NLP keyword matching)
TICKER_KEYWORDS = {}
for ticker in TICKER_MAP:
    clean_sym = ticker.replace('.NS', '')
    # E.g. RELIANCE.NS -> ['reliance', 'reliance.ns']
    TICKER_KEYWORDS[clean_sym] = [clean_sym.lower()]

# For some major companies, add more common aliases
COMMON_ALIASES = {
    'RELIANCE': ['reliance', 'ril', 'mukesh ambani'],
    'TCS': ['tcs', 'tata consultancy'],
    'HDFCBANK': ['hdfc bank', 'hdfcbank'],
    'INFY': ['infosys', 'infy'],
    'ICICIBANK': ['icici bank', 'icicibank'],
    'HINDUNILVR': ['hindustan unilever', 'hul'],
    'ITC': ['itc limited', 'itc '],
    'SBIN': ['sbi', 'state bank of india', 'sbin'],
    'BHARTIARTL': ['bharti airtel', 'airtel'],
    'TATAMOTORS': ['tata motors'],
    'BAJFINANCE': ['bajaj finance'],
    'WIPRO': ['wipro'],
    'LT': ['larsen', 'l&t '],
}

for ticker, aliases in COMMON_ALIASES.items():
    if ticker in TICKER_KEYWORDS:
        TICKER_KEYWORDS[ticker].extend(aliases)

# Provide a plain list of symbols (e.g. ['RELIANCE', 'TCS', ...])
TICKER_SYMBOLS = list(TICKER_KEYWORDS.keys())
\"\"\"

os.makedirs('src/config', exist_ok=True)
with open('src/config/tickers.py', 'w', encoding='utf-8') as f:
    f.write(output)

print('Generated src/config/tickers.py correctly this time!')
