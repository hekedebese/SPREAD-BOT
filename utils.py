import json

def normalize_symbol(symbol):
    return symbol.replace("_", "").replace("-", "").upper()

def load_exceptions():
    try:
        with open("exceptions.json", "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"[ERROR] Не удалось загрузить exceptions.json: {e}")
        return {}

def load_blacklist():
    try:
        with open("blacklist.json", "r") as f:
            return json.load(f)
    except Exception:
        return []

def normalize_and_match(raw_prices):
    """
    1️⃣ Нормализует символы монет
    2️⃣ Группирует синонимы и применяет исключения
    3️⃣ Возвращает словарь: {base_symbol: {source: price}}
    """
    exceptions = load_exceptions()
    global_blacklist = load_blacklist()
    combined = {}

    # --- Синонимы из exceptions ---
    synonym_map = {}
    for base_symbol, settings in exceptions.items():
        synonyms = settings.get("synonyms", [])
        synonym_map[base_symbol] = [base_symbol] + synonyms

    synonym_lookup = {}
    for base, syns in synonym_map.items():
        for s in syns:
            synonym_lookup[s] = base

    # --- Нормализация и группировка ---
    normalized_map = {}  # {base_symbol: {source: {raw_symbol: price}}}
    for source, symbols in raw_prices.items():
        for raw_symbol, price in symbols.items():
            norm_symbol = normalize_symbol(raw_symbol.replace("USDT", "").replace("_USDT", ""))
            base_symbol = synonym_lookup.get(norm_symbol, norm_symbol)

            # 🚫 Пропускаем, если монета в глобальном blacklist
            if norm_symbol in global_blacklist or base_symbol in global_blacklist:
                continue

            # 🚫 Пропускаем, если монета забанена на конкретной бирже
            per_symbol_blacklist = exceptions.get(base_symbol, {}).get("blacklist", [])
            if source in per_symbol_blacklist:
                continue

            if base_symbol not in normalized_map:
                normalized_map[base_symbol] = {}
            if source not in normalized_map[base_symbol]:
                normalized_map[base_symbol][source] = {}
            normalized_map[base_symbol][source][raw_symbol] = price

    # --- Выбор цены с учётом исключений ---
    for base_symbol, sources_data in normalized_map.items():
        for source, variants in sources_data.items():
            exception_symbol = exceptions.get(base_symbol, {}).get(source)
            if exception_symbol and exception_symbol in variants:
                selected_price = variants[exception_symbol]
            else:
                selected_price = list(variants.values())[0]

            combined.setdefault(base_symbol, {})[source] = selected_price

    return combined