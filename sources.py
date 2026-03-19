SOURCES = [
    # === MALAYSIA ===
    # SC Malaysia is a JS-rendered CMS — needs Playwright
    {
        "name": "SC Malaysia Media Releases",
        "url": "https://www.sc.com.my/resources/media/media-release",
        "jurisdiction": "MY",
        "type": "playwright",
        "tags": ["securities", "corporate_governance", "capital_markets"],
    },
    # BNM is behind AWS WAF — needs Playwright to pass JS challenge
    {
        "name": "BNM Press Releases",
        "url": "https://www.bnm.gov.my/publications/press-releases",
        "jurisdiction": "MY",
        "type": "playwright",
        "tags": ["banking", "monetary_policy", "financial_regulation"],
    },
    {
        "name": "BNM Legislation",
        "url": "https://www.bnm.gov.my/legislation",
        "jurisdiction": "MY",
        "type": "playwright",
        "tags": ["banking", "legislation"],
    },
    # Bursa returns 403 to plain requests — needs Playwright
    {
        "name": "Bursa Malaysia Listing Amendments",
        "url": "https://www.bursamalaysia.com/regulation/listing_requirements/main_market/amendments_to_listing_requirements",
        "jurisdiction": "MY",
        "type": "playwright",
        "tags": ["listing_rules", "corporate_governance"],
    },
    # Labuan FSA: index page has category sub-links; parser crawls each
    {
        "name": "Labuan FSA Guidelines",
        "url": "https://www.labuanfsa.gov.my/regulations/guidelines",
        "jurisdiction": "MY-LABUAN",
        "type": "html",
        "tags": ["labuan", "offshore", "financial_regulation"],
    },
    # SSM uses SharePoint; direct press-release list page
    {
        "name": "SSM Announcements",
        "url": "https://www.ssm.com.my/Pages/Publication/Press_Release/Press-Release.aspx",
        "jurisdiction": "MY",
        "type": "html",
        "tags": ["company_law", "corporate"],
    },

    # === HONG KONG ===
    # SFC has working RSS feeds — use rss type with custom UA fetch
    {
        "name": "SFC Press Releases",
        "url": "https://www.sfc.hk/en/RSS-Feeds/Press-releases",
        "jurisdiction": "HK",
        "type": "rss",
        "tags": ["securities", "funds", "enforcement"],
    },
    {
        "name": "SFC Circulars",
        "url": "https://www.sfc.hk/en/RSS-Feeds/Circulars",
        "jurisdiction": "HK",
        "type": "rss",
        "tags": ["intermediaries", "circulars"],
    },
    # HKMA press releases page is JS-rendered — needs Playwright
    {
        "name": "HKMA Press Releases",
        "url": "https://www.hkma.gov.hk/eng/news-and-media/press-releases/",
        "jurisdiction": "HK",
        "type": "playwright",
        "tags": ["banking", "monetary_policy"],
    },
    {
        "name": "HKMA Banking Regulatory Repository",
        "url": "https://brdr.hkma.gov.hk/",
        "jurisdiction": "HK",
        "type": "playwright",
        "tags": ["banking", "circulars", "supervisory_policy"],
    },
    # HKEX Regulatory News page blocks headless browsers; circulars page accessible via HTML
    {
        "name": "HKEX Circulars and Notices",
        "url": "https://www.hkex.com.hk/Services/Circulars-and-Notices/Participant-and-Members-Circulars",
        "jurisdiction": "HK",
        "type": "html",
        "tags": ["listing_rules", "exchange", "circulars"],
    },

    # === UNITED STATES ===
    # SEC press releases RSS — confirmed working at this path
    {
        "name": "SEC Press Releases",
        "url": "https://www.sec.gov/news/pressreleases.rss",
        "jurisdiction": "US",
        "type": "rss",
        "tags": ["securities", "enforcement", "press_release"],
    },
    # SEC rules pages return 403 to plain requests — Playwright handles it
    {
        "name": "SEC Proposed Rules",
        "url": "https://www.sec.gov/rules/proposed/",
        "jurisdiction": "US",
        "type": "playwright",
        "tags": ["rulemaking", "proposed_rules"],
    },
    {
        "name": "SEC Final Rules",
        "url": "https://www.sec.gov/rules/final/",
        "jurisdiction": "US",
        "type": "playwright",
        "tags": ["rulemaking", "final_rules"],
    },
    {
        "name": "SEC Litigation Releases",
        "url": "https://www.sec.gov/litigation/litreleases/",
        "jurisdiction": "US",
        "type": "playwright",
        "tags": ["enforcement", "litigation"],
    },
    {
        "name": "OFAC Recent Actions",
        "url": "https://ofac.treasury.gov/recent-actions",
        "jurisdiction": "US",
        "type": "html",  # RSS retired Jan 2025
        "tags": ["sanctions", "ofac"],
    },
    {
        "name": "FinCEN News",
        "url": "https://www.fincen.gov/news",
        "jurisdiction": "US",
        "type": "html",
        "tags": ["aml", "bsa", "financial_crimes"],
    },
    {
        "name": "FinCEN Advisories",
        "url": "https://www.fincen.gov/resources/advisoriesbulletinsfact-sheets",
        "jurisdiction": "US",
        "type": "html",
        "tags": ["aml", "advisories"],
    },
    # Federal Register — API/RSS geo-blocked; use Playwright on the search page
    {
        "name": "Federal Register - SEC",
        "url": "https://www.federalregister.gov/agencies/securities-and-exchange-commission",
        "jurisdiction": "US",
        "type": "playwright",
        "tags": ["federal_register", "rulemaking"],
    },
]
