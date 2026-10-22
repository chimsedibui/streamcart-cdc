"""Multi-category marketplace catalog spec: category tree, real brand/model pairs, reference prices (VND), variants.

The simulator creates categories and brands through the admin API from this spec, then each
shop lists products drawn from the leaves of the categories it sells. A model keeps its
reference price everywhere; shops price within ±12% of it, as on a real marketplace.
"""

import random
from dataclasses import dataclass, field


@dataclass
class Leaf:
    slug: str
    name: str
    items: list                      # (brand or None, model, reference price VND)
    weight_g: tuple                  # (min, max)
    variants: dict = field(default_factory=dict)  # axis -> values
    premium_axis: str | None = None  # each step on this axis adds 20% to the price


ROOTS = [
    ("dien-thoai-phu-kien", "Điện Thoại & Phụ Kiện", [
        Leaf("dien-thoai", "Điện thoại", [
            ("Samsung", "Galaxy A55 5G", 9_490_000), ("Samsung", "Galaxy S24", 19_990_000),
            ("Apple", "iPhone 15", 19_990_000), ("Apple", "iPhone 13", 12_990_000),
            ("Xiaomi", "Redmi Note 13", 4_890_000), ("Xiaomi", "Poco X6 Pro", 8_490_000),
            ("OPPO", "Reno11 F", 8_990_000), ("vivo", "V30e", 9_490_000), ("realme", "C67", 4_290_000)],
            (170, 240), {"Màu": ["Đen", "Trắng", "Xanh"], "Dung lượng": ["128GB", "256GB"]}, "Dung lượng"),
        Leaf("tai-nghe", "Tai nghe", [
            ("Sony", "WH-1000XM5", 7_490_000), ("JBL", "Tune 520BT", 1_090_000), ("Apple", "AirPods 3", 4_190_000),
            ("Soundcore", "Life Q30", 1_590_000), ("Baseus", "Bowie WM02", 390_000), ("Sony", "WF-C700N", 2_490_000)],
            (40, 300), {"Màu": ["Đen", "Trắng", "Be"]}),
        Leaf("sac-cap", "Sạc & Cáp", [
            ("Anker", "Sạc nhanh 20W", 290_000), ("Anker", "Pin dự phòng 10000mAh", 590_000),
            ("Baseus", "Sạc GaN 65W", 690_000), ("Ugreen", "Cáp Type-C 1m", 119_000),
            ("Ugreen", "Cáp Lightning 2m", 189_000), ("Xiaomi", "Pin dự phòng 20000mAh", 549_000)],
            (40, 420), {"Màu": ["Đen", "Trắng"]}),
    ]),
    ("may-tinh-laptop", "Máy Tính & Laptop", [
        Leaf("laptop", "Laptop", [
            ("ASUS", "Vivobook 15 OLED", 15_990_000), ("ASUS", "TUF Gaming F15", 21_490_000),
            ("Dell", "Inspiron 14 5440", 18_990_000), ("Lenovo", "IdeaPad Slim 5", 16_990_000),
            ("HP", "Pavilion 15", 17_490_000), ("Acer", "Aspire 7", 15_490_000),
            ("Apple", "MacBook Air M2", 24_990_000)],
            (1300, 2300), {"Cấu hình": ["8GB/256GB", "16GB/512GB"]}, "Cấu hình"),
        Leaf("linh-kien-pc", "Linh kiện PC", [
            ("AMD", "Ryzen 5 7600", 5_290_000), ("Intel", "Core i5-14400F", 4_990_000),
            ("ASUS", "Dual RTX 4060 8GB", 8_490_000), ("Corsair", "Vengeance 32GB DDR5", 2_890_000),
            ("Samsung", "SSD 990 PRO 1TB", 3_190_000), ("Kingston", "Fury Beast 16GB DDR4", 1_090_000)],
            (80, 1200)),
        Leaf("chuot-ban-phim", "Chuột & Bàn phím", [
            ("Logitech", "G304", 690_000), ("Logitech", "MX Master 3S", 2_490_000),
            ("Razer", "DeathAdder V3", 1_890_000), ("Keychron", "K2 Pro", 2_390_000),
            ("Akko", "3068B Plus", 1_590_000), ("Rapoo", "M100", 159_000)],
            (90, 1100), {"Màu": ["Đen", "Trắng", "Hồng"]}),
    ]),
    ("thoi-trang-nu", "Thời Trang Nữ", [
        Leaf("vay-dam", "Váy đầm", [
            ("IVY moda", "Đầm suông cổ vuông", 890_000), ("Elise", "Đầm công sở", 1_190_000),
            (None, "Váy hoa nhí", 189_000), (None, "Váy maxi đi biển", 259_000), (None, "Chân váy xếp ly", 159_000)],
            (200, 450), {"Size": ["S", "M", "L"], "Màu": ["Đen", "Be", "Hồng"]}),
        Leaf("ao-nu", "Áo nữ", [
            ("YODY", "Áo sơ mi lụa", 399_000), ("Uniqlo", "Áo thun cổ tròn", 293_000),
            (None, "Áo croptop", 99_000), (None, "Áo len mỏng", 189_000)],
            (150, 350), {"Size": ["S", "M", "L"], "Màu": ["Trắng", "Đen", "Xanh"]}),
    ]),
    ("thoi-trang-nam", "Thời Trang Nam", [
        Leaf("ao-thun-nam", "Áo thun nam", [
            ("Coolmate", "Áo thun cotton Compact", 179_000), ("Coolmate", "Áo polo Excool", 299_000),
            ("Routine", "Áo thun oversize", 245_000), (None, "Áo tank top thể thao", 89_000)],
            (180, 320), {"Size": ["M", "L", "XL"], "Màu": ["Đen", "Trắng", "Xám"]}),
        Leaf("quan-nam", "Quần nam", [
            ("Routine", "Quần jean slim fit", 545_000), ("Levi's", "Quần jean 511", 1_490_000),
            ("Coolmate", "Quần short thể thao", 199_000), (None, "Quần kaki", 259_000)],
            (300, 700), {"Size": ["29", "30", "31", "32"]}),
        Leaf("giay-nam", "Giày nam", [
            ("Biti's", "Hunter X", 990_000), ("Nike", "Air Force 1", 2_929_000), ("Adidas", "Ultraboost Light", 4_800_000),
            ("Ananas", "Vintas Saigon", 620_000)],
            (600, 1100), {"Size": ["40", "41", "42", "43"]}),
    ]),
    ("sac-dep", "Sắc Đẹp", [
        Leaf("trang-diem", "Trang điểm", [
            ("Maybelline", "Son kem lì SuperStay", 229_000), ("3CE", "Son thỏi Velvet", 390_000),
            ("Romand", "Son tint Juicy Lasting", 189_000), ("Black Rouge", "Son kem Air Fit", 159_000),
            ("L'Oréal", "Cushion Infallible", 359_000)],
            (30, 150), {"Màu": ["01", "03", "05", "07"]}),
        Leaf("cham-soc-da", "Chăm sóc da", [
            ("La Roche-Posay", "Kem chống nắng Anthelios SPF50", 495_000), ("Cocoon", "Nước tẩy trang bí đao 500ml", 295_000),
            ("The Ordinary", "Serum Niacinamide 10%", 320_000), ("Klairs", "Toner Supple Preparation", 349_000),
            ("Bioderma", "Nước tẩy trang Sensibio 500ml", 445_000)],
            (120, 600)),
    ]),
    ("nha-cua-doi-song", "Nhà Cửa & Đời Sống", [
        Leaf("do-gia-dung", "Đồ gia dụng", [
            ("Philips", "Nồi chiên không dầu HD9200", 2_090_000), ("Lock&Lock", "Nồi chiên không dầu 5.2L", 1_490_000),
            ("Sunhouse", "Nồi cơm điện 1.8L", 690_000), ("Xiaomi", "Máy lọc không khí 4 Lite", 2_990_000),
            ("Lock&Lock", "Bình giữ nhiệt 500ml", 299_000)],
            (400, 6500)),
        Leaf("chan-ga-goi", "Chăn ga gối", [
            ("Everon", "Bộ chăn ga cotton", 1_890_000), ("Hanvico", "Gối cao su non", 459_000),
            (None, "Chăn hè đũi", 259_000), (None, "Ga chống thấm", 149_000)],
            (800, 3000), {"Kích thước": ["1m6", "1m8"]}, "Kích thước"),
        Leaf("bep", "Dụng cụ nhà bếp", [
            ("Sunhouse", "Chảo chống dính 26cm", 249_000), ("Lock&Lock", "Bộ hộp thực phẩm 5 món", 359_000),
            (None, "Bộ dao inox 6 món", 179_000), (None, "Thớt gỗ nghiến", 129_000)],
            (150, 1800)),
    ]),
    ("me-be", "Mẹ & Bé", [
        Leaf("ta-bim", "Tã bỉm", [
            ("Bobby", "Tã quần Bobby", 339_000), ("Huggies", "Tã dán Huggies", 315_000),
            ("Merries", "Tã quần Merries", 399_000), ("Moony", "Tã dán Moony Natural", 459_000)],
            (1500, 3500), {"Size": ["M", "L", "XL"]}),
        Leaf("sua-bot", "Sữa bột", [
            ("Vinamilk", "Optimum Gold 800g", 399_000), ("Abbott", "Grow Plus 900g", 529_000),
            ("Nutifood", "GrowPLUS+ 850g", 345_000), ("Meiji", "Meiji Step 800g", 549_000)],
            (900, 1300)),
    ]),
    ("bach-hoa-online", "Bách Hóa Online", [
        Leaf("do-an-vat", "Đồ ăn vặt", [
            ("Oishi", "Snack tôm cay", 15_000), ("Lay's", "Khoai tây chiên 95g", 25_000),
            ("Orion", "Chocopie hộp 12", 59_000), (None, "Khô gà lá chanh 500g", 129_000),
            (None, "Hạt điều rang muối 500g", 189_000)],
            (60, 600)),
        Leaf("ca-phe-tra", "Cà phê & Trà", [
            ("Trung Nguyên", "Cà phê Sáng Tạo 5 500g", 189_000), ("Highlands", "Cà phê phin giấy", 99_000),
            ("Phúc Long", "Trà ô long 200g", 165_000), ("Nescafé", "Cà phê 3in1 hộp 20 gói", 69_000)],
            (200, 600)),
    ]),
]

LEAVES = [(root_slug, leaf) for root_slug, _, leaves in ROOTS for leaf in leaves]


def brands() -> list:
    return sorted({brand for _, leaf in LEAVES for brand, _, _ in leaf.items if brand})


def _round_price(v: float) -> int:
    return max(1_000, int(round(v / 1_000)) * 1_000)


def make_product(leaf: Leaf, sku_prefix: str) -> dict:
    """One listing of a real model: shop price within ±12% of reference, up to 4 variants."""
    brand, model, reference = random.choice(leaf.items)
    base = reference * random.uniform(0.88, 1.12)
    combos = [()]
    for axis, values in leaf.variants.items():
        combos = [c + ((axis, v),) for c in combos for v in values]
    combos = random.sample(combos, min(len(combos), random.randint(1, 4)))
    weight = random.randint(*leaf.weight_g)
    discounted = random.random() < 0.6

    skus = []
    for i, combo in enumerate(combos):
        price = base
        for axis, value in combo:
            if axis == leaf.premium_axis:
                price *= 1 + 0.2 * leaf.variants[axis].index(value)
        price = _round_price(price)
        list_price = _round_price(price * (1 + random.uniform(0.1, 0.45))) if discounted else price
        skus.append({
            "sku_code": f"{sku_prefix}-{i + 1}",
            "variant_name": " / ".join(v for _, v in combo) or "Mặc định",
            "list_price": list_price,
            "price": price,
            "weight_g": weight,
            # cheaper goods are stocked deeper
            "initial_stock": random.randint(20, 80) if price > 5_000_000 else random.randint(60, 400),
            "reorder_point": random.choice([5, 10, 15, 20]),
        })
    name = f"{brand} {model}" if brand and not model.startswith(brand) else model
    return {
        "category_slug": leaf.slug,
        "brand": brand,
        "name": name,
        "description": f"{name} chính hãng, giao nhanh toàn quốc.",
        "skus": skus,
    }
