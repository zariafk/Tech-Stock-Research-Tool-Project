"""Top 100 tech companies by market capitalization for all of our pipelines"""

# Map Ticker to Full Company Name for accurate keyword matching
tech_universe = {
    # Mega Cap Tech
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "NVDA": "NVIDIA",
    "AMZN": "Amazon",
    "GOOGL": "Google",
    "META": "Meta",
    "TSLA": "Tesla",

    # Large Cap Platforms / Software
    "NFLX": "Netflix",
    "ADBE": "Adobe",
    "CRM": "Salesforce",
    "ORCL": "Oracle",
    "IBM": "IBM",
    "INTU": "Intuit",
    "NOW": "ServiceNow",
    "CSCO": "Cisco",

    # Semiconductors
    # "AMD": "AMD",
    # "INTC": "Intel",
    # "AVGO": "Broadcom",
    # "QCOM": "Qualcomm",
    # "TXN": "Texas Instruments",
    # "MU": "Micron",
    # "LRCX": "Lam Research",
    # "AMAT": "Applied Materials",
    # "KLAC": "KLA",
    # "MCHP": "Microchip Technology",
    # "NXPI": "NXP Semiconductors",
    # "MPWR": "Monolithic Power",
    # "SWKS": "Skyworks Solutions",
    # "QRVO": "Qorvo",
    # "TER": "Teradyne",
    # "ON": "ON Semiconductor",

    # Networking / Infrastructure
    # "ANET": "Arista Networks",
    # "CIEN": "Ciena",
    # "JNPR": "Juniper Networks",

    # Hardware / Storage
    # "DELL": "Dell",
    # "HPQ": "HP",
    # "NTAP": "NetApp",
    # "STX": "Seagate",
    # "WDC": "Western Digital",

    # Cloud / SaaS
    # "SNOW": "Snowflake",
    # "PLTR": "Palantir",
    # "CRWD": "CrowdStrike",
    # "ZS": "Zscaler",
    # "OKTA": "Okta",
    # "DDOG": "Datadog",
    # "NET": "Cloudflare",
    # "MDB": "MongoDB",
    # "HUBS": "HubSpot",
    # "SHOP": "Shopify",
    # "TEAM": "Atlassian",
    # "WDAY": "Workday",
    # "VEEV": "Veeva",
    # "DOCU": "DocuSign",
    # "TWLO": "Twilio",
    # "SPLK": "Splunk",
    # "ESTC": "Elastic",
    # "FSLY": "Fastly",
    # "U": "Unity",
    # "PATH": "UiPath",
    # "APP": "AppLovin",
    # "PCOR": "Procore",
    # "CFLT": "Confluent",

    # Cybersecurity
    # "PANW": "Palo Alto Networks",
    # "FTNT": "Fortinet",

    # Consumer Tech / Internet
    "UBER": "Uber",
    "ABNB": "Airbnb",
    "DASH": "DoorDash",
    "ROKU": "Roku",
    "SPOT": "Spotify",
    "PYPL": "PayPal",
    "SQ": "Block",
    "ETSY": "Etsy",

    # Fintech
    "AFRM": "Affirm",
    "COIN": "Coinbase",
    "HOOD": "Robinhood",

    # Data / AI / Analytics
    # "SNPS": "Synopsys",
    # "CDNS": "Cadence",
    # "AKAM": "Akamai",

    # Gaming / Interactive
    # "EA": "Electronic Arts",
    # "TTWO": "Take-Two",
    # "RBLX": "Roblox",

    # Emerging / Growth Tech
    # "AI": "C3.ai",
    # "BILL": "Bill.com",
    # "GTLB": "GitLab",
    # "MNDY": "monday.com",
    # "S": "SentinelOne",
    # "NEWR": "New Relic",
    # "SMAR": "Smartsheet",
    # "ASAN": "Asana",

    # Additional large/mid cap tech
    # "ZM": "Zoom",
    # "DOCN": "DigitalOcean",
    # "PINS": "Pinterest",
    # "LYFT": "Lyft",
    # "IOT": "Samsara",
    # "CRNC": "Cerence",
    # "VRNS": "Varonis"
}
