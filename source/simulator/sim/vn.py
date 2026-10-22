"""Vietnamese names, places and contact details for synthetic buyers and shops."""

import random
import unicodedata

# (province, share of buyers, share of shops, districts) — provinces must match the Source API's REGIONS
PROVINCES = [
    ("TP.HCM", 0.32, 0.45, ["Quận 1", "Quận 3", "Quận 7", "Bình Thạnh", "Gò Vấp", "Tân Bình", "Thủ Đức", "Bình Tân"]),
    ("Hà Nội", 0.25, 0.35, ["Ba Đình", "Hoàn Kiếm", "Cầu Giấy", "Đống Đa", "Thanh Xuân", "Hà Đông", "Long Biên",
                            "Nam Từ Liêm"]),
    ("Đà Nẵng", 0.05, 0.04, ["Hải Châu", "Thanh Khê", "Sơn Trà", "Ngũ Hành Sơn"]),
    ("Hải Phòng", 0.04, 0.03, ["Ngô Quyền", "Lê Chân", "Hồng Bàng"]),
    ("Bình Dương", 0.05, 0.04, ["Thủ Dầu Một", "Dĩ An", "Thuận An"]),
    ("Đồng Nai", 0.04, 0.02, ["Biên Hòa", "Long Khánh"]),
    ("Cần Thơ", 0.03, 0.02, ["Ninh Kiều", "Bình Thủy"]),
    ("Khánh Hòa", 0.02, 0.01, ["Nha Trang", "Cam Ranh"]),
    ("Thừa Thiên Huế", 0.02, 0.01, ["TP Huế", "Hương Thủy"]),
    ("Lâm Đồng", 0.02, 0.01, ["Đà Lạt", "Bảo Lộc"]),
    ("Bà Rịa - Vũng Tàu", 0.02, 0.0, ["Vũng Tàu", "Bà Rịa"]),
    ("Quảng Ninh", 0.02, 0.0, ["Hạ Long", "Cẩm Phả"]),
    ("Bắc Ninh", 0.02, 0.01, ["TP Bắc Ninh", "Từ Sơn"]),
    ("Nghệ An", 0.02, 0.0, ["TP Vinh", "Cửa Lò"]),
    ("Thanh Hóa", 0.02, 0.0, ["TP Thanh Hóa", "Sầm Sơn"]),
    ("Long An", 0.03, 0.01, ["Tân An", "Bến Lức"]),
    ("An Giang", 0.03, 0.0, ["Long Xuyên", "Châu Đốc"]),
]

STREETS = ["Lê Lợi", "Nguyễn Huệ", "Trần Hưng Đạo", "Lý Thường Kiệt", "Hai Bà Trưng", "Điện Biên Phủ",
           "Võ Văn Tần", "Cách Mạng Tháng 8", "Nguyễn Trãi", "Lê Duẩn", "Xuân Thủy", "Trần Duy Hưng",
           "Nguyễn Văn Linh", "Bạch Đằng", "Hùng Vương", "Quang Trung", "Phan Chu Trinh", "Nguyễn Thị Minh Khai",
           "Pasteur", "Lạc Long Quân", "Phạm Văn Đồng", "Kim Mã", "Láng Hạ", "Tô Hiến Thành"]

LAST_NAMES = [("Nguyễn", 38), ("Trần", 11), ("Lê", 9), ("Phạm", 7), ("Hoàng", 5), ("Huỳnh", 4), ("Phan", 4),
              ("Vũ", 4), ("Võ", 4), ("Đặng", 2), ("Bùi", 2), ("Đỗ", 2), ("Hồ", 2), ("Ngô", 2), ("Dương", 1),
              ("Lý", 1)]
MIDDLE_NAMES = ["Văn", "Thị", "Minh", "Ngọc", "Thanh", "Đức", "Thu", "Hoàng", "Quang", "Anh", "Gia", "Bảo"]
FIRST_NAMES = ["An", "Bình", "Chi", "Dũng", "Giang", "Hà", "Hải", "Hạnh", "Hiếu", "Hoa", "Huy", "Hương", "Khánh",
               "Lan", "Linh", "Long", "Mai", "Minh", "Nam", "Ngọc", "Nhung", "Phong", "Phúc", "Quân", "Quỳnh",
               "Sơn", "Tâm", "Thảo", "Trang", "Tuấn", "Vy", "Yến", "Đạt", "Khoa", "Nhi", "Thư"]
EMAIL_DOMAINS = [("gmail.com", 70), ("yahoo.com", 8), ("outlook.com", 7), ("icloud.com", 6), ("hotmail.com", 4),
                 ("fpt.vn", 3), ("vnn.vn", 2)]
PHONE_PREFIXES = ["032", "033", "034", "035", "036", "037", "038", "039", "086", "096", "097", "098", "070", "076",
                  "077", "078", "079", "089", "090", "093", "081", "082", "083", "084", "085", "088", "091", "094"]

SHOP_WORDS = ["Minh Phát", "Hưng Thịnh", "An Khang", "Phú Quý", "Tâm Đức", "Gia Bảo", "Thành Công", "Kim Long",
              "Hoàng Gia", "Việt Tiến", "Nhà Xinh", "Bếp Việt", "Mẹ Bông", "Góc Nhỏ", "Tiệm Xinh", "Sài Gòn",
              "Hà Thành", "Đại Phát", "Thiên Ân", "Ngọc Trâm"]
SHOP_SUFFIXES = ["Store", "Shop", "Official", "House", "Mart", "24h", "Center", "Outlet"]


def ascii_slug(text: str) -> str:
    text = text.replace("đ", "d").replace("Đ", "D")
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return "".join(ch for ch in text.lower() if ch.isalnum())


def weighted(pairs):
    items, weights = zip(*pairs)
    return random.choices(items, weights=weights)[0]


def full_name() -> str:
    return f"{weighted(LAST_NAMES)} {random.choice(MIDDLE_NAMES)} {random.choice(FIRST_NAMES)}"


def email_for(name: str) -> str:
    parts = name.split()
    return f"{ascii_slug(parts[-1])}.{ascii_slug(parts[0])}{random.randint(1, 99999)}@{weighted(EMAIL_DOMAINS)}"


def phone() -> str:
    return random.choice(PHONE_PREFIXES) + f"{random.randint(0, 9_999_999):07d}"


def buyer_province():
    return random.choices(PROVINCES, weights=[p[1] for p in PROVINCES])[0]


def shop_province() -> str:
    return random.choices(PROVINCES, weights=[p[2] for p in PROVINCES])[0][0]


def address(name: str) -> dict:
    province, _, _, districts = buyer_province()
    return {
        "recipient_name": name,
        "phone": phone(),
        "street": f"{random.randint(1, 450)} {random.choice(STREETS)}",
        "district": random.choice(districts),
        "province": province,
    }


def shop_name(i: int) -> str:
    return f"{random.choice(SHOP_WORDS)} {random.choice(SHOP_SUFFIXES)} {i:03d}"


# Same mapping as the Source API's rate card (used for carrier transit times)
REGIONS = {
    "Hà Nội": "NORTH", "Hải Phòng": "NORTH", "Quảng Ninh": "NORTH", "Bắc Ninh": "NORTH", "Thanh Hóa": "NORTH",
    "Nghệ An": "CENTRAL", "Thừa Thiên Huế": "CENTRAL", "Đà Nẵng": "CENTRAL", "Khánh Hòa": "CENTRAL",
    "Lâm Đồng": "CENTRAL",
    "TP.HCM": "SOUTH", "Bình Dương": "SOUTH", "Đồng Nai": "SOUTH", "Bà Rịa - Vũng Tàu": "SOUTH",
    "Long An": "SOUTH", "Cần Thơ": "SOUTH", "An Giang": "SOUTH",
}


def route(from_province: str, to_province: str) -> str:
    if from_province == to_province:
        return "SAME_PROVINCE"
    return "SAME_REGION" if REGIONS.get(from_province) == REGIONS.get(to_province) else "OTHER"
