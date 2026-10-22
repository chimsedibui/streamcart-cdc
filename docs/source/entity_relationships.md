# ERD theo nhóm nghiệp vụ

[Tài liệu dự án](../README.md) / [Kiến trúc](README.md)

Các sơ đồ dùng tên bảng/cột thật nhưng **chỉ chọn một số cột** để dễ đọc. Bảng ngữ cảnh chỉ hiển thị PK; cột FK dùng làm nhãn cạnh có thể được ẩn trong ô ngữ cảnh. Danh sách đầy đủ 38 bảng và 319 cột nằm ở [erDiagram.txt](erDiagram.txt). Xem [quy ước cardinality và giới hạn ERD](README.md#cách-đọc-erd) trước khi suy ra quy tắc nghiệp vụ từ cạnh nối.

Các cạnh bên dưới là 57 FK vật lý của schema, được chia nhóm và có thể lặp giữa các nhóm để giữ ngữ cảnh. Không biểu diễn quan hệ ClickHouse bằng FK PostgreSQL.

## 1. Seller, catalog và tồn kho

Seller là chủ thể bán; shop là đơn vị vận hành có tier, địa điểm gửi và commission rate. Product là listing của shop, SKU mới là cấp có giá, khối lượng và tồn kho. Product có một category; brand là tùy chọn nên cạnh brand → product cho phép không có brand. Categories có parent nullable, cho phép node gốc.

Một SKU có tối đa một dòng inventory do sku_id đồng thời là PK và FK. API listing tạo tồn kho, nhưng cạnh ERD vẫn dùng 0..1 ở phía inventory: riêng FK không ép mọi SKU phải có inventory. Inventory movements và price_history có khóa BIGINT tăng dần và lưu lịch sử; ref_type/ref_id của movement là liên kết nghiệp vụ đa hình, không vẽ thành FK tới order/return.

Các bảng ngữ cảnh: không có.

```mermaid
erDiagram
    sellers {
        uuid id PK
        varchar legal_name
    }
    shops {
        uuid id PK
        uuid seller_id FK
        varchar name UK
        varchar shop_tier
        int4 commission_rate_bps
    }
    categories {
        uuid id PK
        uuid parent_id FK "nullable"
        varchar name
        varchar slug UK
    }
    brands {
        uuid id PK
        varchar name UK
    }
    products {
        uuid id PK
        uuid shop_id FK
        uuid category_id FK
        uuid brand_id FK "nullable"
        varchar name
        varchar status
    }
    product_skus {
        uuid id PK
        uuid product_id FK
        varchar sku_code UK
        int8 price
        int4 weight_g
    }
    price_history {
        int8 id PK
        uuid sku_id FK
        int8 old_price "nullable"
        int8 new_price
        timestamptz created_at
    }
    inventory {
        uuid sku_id PK, FK
        uuid shop_id FK
        int4 on_hand
        int4 reserved
        int4 available
    }
    inventory_movements {
        int8 id PK
        uuid sku_id FK
        varchar movement_type
        int4 on_hand_delta
        int4 reserved_delta
        varchar ref_type
        uuid ref_id
    }
    brands |o--o{ products : brand_id
    categories |o--o{ categories : parent_id
    categories ||--o{ products : category_id
    product_skus ||--o| inventory : sku_id
    product_skus ||--o{ inventory_movements : sku_id
    product_skus ||--o{ price_history : sku_id
    products ||--o{ product_skus : product_id
    sellers ||--o{ shops : seller_id
    shops ||--o{ inventory : shop_id
    shops ||--o{ products : shop_id
```

## 2. Buyer, giỏ hàng và checkout theo shop

Một customer có nhiều cart theo thời gian; partial unique index giới hạn một cart ACTIVE, không phải một cart cho cả đời tài khoản. Một cart có thể chưa checkout; checkouts.cart_id unique nên tối đa một checkout cho cart đó. Checkout tách nhiều order theo shop, rồi mỗi order có nhiều order item.

checkouts.address_id là FK tới sổ địa chỉ. Order lại giữ ship_to_name/address/province dạng snapshot để sửa địa chỉ hiện tại không thay đổi đơn cũ. Unit price và net amount cũng là snapshot tại checkout. Order item có campaign_item_id nullable vì có thể được bán ngoài flash sale. Lịch sử trạng thái là dữ liệu append-only riêng, không cần đoán từ updated_at.

Các bảng ngữ cảnh: `product_skus`, `products`, `shops`, `flash_sale_items`.

```mermaid
erDiagram
    customers {
        uuid id PK
        varchar full_name
        varchar tier
        int8 total_spent
    }
    customer_addresses {
        uuid id PK
        uuid customer_id FK
        varchar province
        bool is_default
    }
    carts {
        uuid id PK
        uuid customer_id FK
        varchar status
    }
    cart_items {
        uuid id PK
        uuid cart_id FK
        uuid sku_id FK
        int4 quantity
    }
    checkouts {
        uuid id PK
        uuid customer_id FK
        uuid cart_id FK, UK
        uuid address_id FK
        varchar payment_method
        varchar status
        int8 grand_total
    }
    orders {
        uuid id PK
        uuid checkout_id FK
        uuid customer_id FK
        uuid shop_id FK
        varchar status
        int8 subtotal
        int8 total
        varchar ship_to_address
    }
    order_items {
        uuid id PK
        uuid order_id FK
        uuid sku_id FK
        uuid product_id FK
        int4 quantity
        int8 unit_price
        int8 net_amount
        uuid campaign_item_id FK "nullable"
    }
    order_status_history {
        int8 id PK
        uuid order_id FK
        varchar from_status "nullable"
        varchar to_status
        varchar actor
        timestamptz created_at
    }
    product_skus {
        uuid id PK
    }
    products {
        uuid id PK
    }
    shops {
        uuid id PK
    }
    flash_sale_items {
        uuid id PK
    }
    carts ||--o{ cart_items : cart_id
    carts ||--o| checkouts : cart_id
    checkouts ||--o{ orders : checkout_id
    customer_addresses ||--o{ checkouts : address_id
    customers ||--o{ carts : customer_id
    customers ||--o{ checkouts : customer_id
    customers ||--o{ customer_addresses : customer_id
    customers ||--o{ orders : customer_id
    flash_sale_items |o--o{ order_items : campaign_item_id
    orders ||--o{ order_items : order_id
    orders ||--o{ order_status_history : order_id
    product_skus ||--o{ cart_items : sku_id
    product_skus ||--o{ order_items : sku_id
    products ||--o{ order_items : product_id
    shops ||--o{ orders : shop_id
```

## 3. Voucher, campaign và quota

Voucher scope PLATFORM có shop_id NULL; voucher SHOP phải gắn shop. CHECK của schema cưỡng chế điều kiện này, trong khi cạnh ERD chỉ thể hiện FK nullable. Campaign gom các flash_sale_items theo SKU; unique (campaign_id, sku_id) ngăn SKU xuất hiện hai lần trong cùng campaign.

Voucher usages nối voucher và customer với checkout, có order_id khi áp vào order shop. Nó ghi dấu áp dụng/đảo hiệu lực thay vì dùng một promotion_id trên order. Voucher quota chia thành nhiều dòng với PK ghép voucher_id + slot; bảng slot hỗ trợ đồng thời ở nguồn và bị loại khỏi CDC. Flash-sale quota/sold thuộc flash_sale_items, không dùng chung cơ chế slot của voucher.

Các bảng ngữ cảnh: `shops`, `product_skus`, `customers`, `checkouts`, `orders`.

```mermaid
erDiagram
    vouchers {
        uuid id PK
        varchar scope
        uuid shop_id FK "nullable"
        varchar kind
        int8 value
        int4 usage_limit
    }
    voucher_quota_slots {
        uuid voucher_id PK, FK
        int2 slot PK
        int4 capacity
        int4 used
    }
    campaigns {
        uuid id PK
        varchar kind
        timestamptz starts_at
        timestamptz ends_at
    }
    flash_sale_items {
        uuid id PK
        uuid campaign_id FK
        uuid sku_id FK
        int8 sale_price
        int4 quota
        int4 sold
    }
    voucher_usages {
        uuid id PK
        uuid voucher_id FK
        uuid customer_id FK
        uuid checkout_id FK
        uuid order_id FK "nullable"
        int8 amount
        varchar status
    }
    shops {
        uuid id PK
    }
    product_skus {
        uuid id PK
    }
    customers {
        uuid id PK
    }
    checkouts {
        uuid id PK
    }
    orders {
        uuid id PK
    }
    campaigns ||--o{ flash_sale_items : campaign_id
    checkouts ||--o{ voucher_usages : checkout_id
    customers ||--o{ voucher_usages : customer_id
    orders |o--o{ voucher_usages : order_id
    product_skus ||--o{ flash_sale_items : sku_id
    shops |o--o{ vouchers : shop_id
    vouchers ||--o{ voucher_quota_slots : voucher_id
    vouchers ||--o{ voucher_usages : voucher_id
```

## 4. Payment, fulfillment và customer return

Payment luôn gắn checkout; order_id tùy chọn phân biệt thu tiền theo checkout với COD theo order. Refund luôn gắn payment nhưng order_id và return_id có thể NULL, như tiền online đến muộn sau khi checkout hết hạn. Trạng thái refund nằm ở refunds, không phải một payment_status REFUNDED.

Order có tối đa một return request, mỗi order item có tối đa một return line. Refund và shipment có FK tới return được khai báo qua ALTER TABLE trong schema, vì vậy phải đọc cả phần khai báo sau CREATE TABLE khi dựng ERD. Một order có thể có một shipment FORWARD và một RETURN nhờ unique (order_id, direction); ERD orders → shipments vẫn là nhiều, không phải 1:1.

Carrier events lưu event_time và applied để giữ dấu callback cũ mà không kéo lùi trạng thái. Customer return có inspection và hoàn tiền riêng; giao thất bại trả về seller không tự động tạo một customer return request.

Các bảng ngữ cảnh: `checkouts`, `orders`, `order_items`, `customers`, `shops`.

```mermaid
erDiagram
    payments {
        uuid id PK
        uuid checkout_id FK
        uuid order_id FK "nullable"
        varchar method
        int8 amount
        varchar status
    }
    refunds {
        uuid id PK
        uuid payment_id FK
        uuid order_id FK "nullable"
        uuid return_id FK "nullable"
        int8 amount
        varchar reason
        varchar status
    }
    shipments {
        uuid id PK
        uuid order_id FK
        uuid return_id FK "nullable"
        varchar direction
        varchar status
        timestamptz last_event_time "nullable"
    }
    shipment_events {
        int8 id PK
        uuid shipment_id FK
        varchar event_type
        timestamptz event_time
        bool applied
    }
    returns {
        uuid id PK
        uuid order_id FK, UK
        uuid customer_id FK
        uuid shop_id FK
        varchar reason
        varchar status
        timestamptz respond_by
    }
    return_items {
        uuid id PK
        uuid return_id FK
        uuid order_item_id FK, UK
        int4 quantity
        int8 refund_amount
        varchar condition "nullable"
    }
    checkouts {
        uuid id PK
    }
    orders {
        uuid id PK
    }
    order_items {
        uuid id PK
    }
    customers {
        uuid id PK
    }
    shops {
        uuid id PK
    }
    checkouts ||--o{ payments : checkout_id
    customers ||--o{ returns : customer_id
    order_items ||--o| return_items : order_item_id
    orders |o--o{ payments : order_id
    orders |o--o{ refunds : order_id
    orders ||--o| returns : order_id
    orders ||--o{ shipments : order_id
    payments ||--o{ refunds : payment_id
    returns |o--o{ refunds : return_id
    returns ||--o{ return_items : return_id
    returns |o--o{ shipments : return_id
    shipments ||--o{ shipment_events : shipment_id
    shops ||--o{ returns : shop_id
```

## 5. Seller settlement, ví và review

Seller ledger gắn shop và có thể gắn order hoặc payout theo loại entry. SALE/COMMISSION/PAYMENT_FEE ghi nhận quyết toán đơn, PAYOUT ghi tiền trả cho shop. Những liên kết nullable không có nghĩa mọi entry đồng thời thuộc cả order lẫn payout.

shop_wallets có PK=FK shop_id. wallet_pending có PK=FK entry_id, nghĩa là mỗi ledger entry có tối đa một dòng pending; shop_id trong pending là cột dữ liệu nhưng không có FK vật lý riêng. Roll-up cộng pending vào balance theo lô; không suy ra balance luôn bằng tổng mọi ledger ngay sau mỗi transaction hoàn tất order.

Review gắn với order item duy nhất và mang thêm order/product/shop/customer để đọc, phân quyền và tổng hợp. Các FK riêng chỉ bảo đảm bản ghi cha tồn tại; API còn phải bảo đảm các ID này thuộc cùng một giao dịch mua.

Các bảng ngữ cảnh: `shops`, `orders`, `order_items`, `products`, `customers`.

```mermaid
erDiagram
    seller_ledger_entries {
        int8 id PK
        uuid shop_id FK
        uuid order_id FK "nullable"
        uuid payout_id FK "nullable"
        varchar entry_type
        int8 amount
    }
    seller_payouts {
        uuid id PK
        uuid shop_id FK
        int8 amount
        varchar status
    }
    shop_wallets {
        uuid shop_id PK, FK
        int8 balance
    }
    wallet_pending {
        int8 entry_id PK, FK
        uuid shop_id
        int8 amount
    }
    product_reviews {
        uuid id PK
        uuid order_item_id FK, UK
        uuid order_id FK
        uuid product_id FK
        uuid shop_id FK
        uuid customer_id FK
        int2 rating
    }
    shops {
        uuid id PK
    }
    orders {
        uuid id PK
    }
    order_items {
        uuid id PK
    }
    products {
        uuid id PK
    }
    customers {
        uuid id PK
    }
    customers ||--o{ product_reviews : customer_id
    order_items ||--o| product_reviews : order_item_id
    orders ||--o{ product_reviews : order_id
    orders |o--o{ seller_ledger_entries : order_id
    products ||--o{ product_reviews : product_id
    seller_ledger_entries ||--o| wallet_pending : entry_id
    seller_payouts |o--o{ seller_ledger_entries : payout_id
    shops ||--o{ product_reviews : shop_id
    shops ||--o{ seller_ledger_entries : shop_id
    shops ||--o{ seller_payouts : shop_id
    shops ||--o| shop_wallets : shop_id
```

## 6. Bảng điều khiển và tích hợp

Đây là những bảng nguồn thật, không phải mart. sim_clock điều khiển business time; idempotency_keys giữ response thành công cho retry; cdc_heartbeat giúp slot tiến lên ở nguồn ít thay đổi. Cả ba bị loại khỏi capture. wallet_pending và voucher_quota_slots ở nhóm trước cũng bị loại.

debezium_signals được capture nhưng không có silver nghiệp vụ. Outbox được capture rồi Event Router chuyển sang events.<aggregate>; aggregateid không phải một FK đa bảng. Việc không có cạnh trên sơ đồ này là có chủ đích, không có nghĩa các bảng không tham gia luồng vận hành.

Identity nằm ở database riêng, không đưa vào ERD enterprise_db: users có memberships và refresh_tokens; resource_id của membership liên hệ customer/shop qua ứng dụng. Audit user_id cũng không được suy thành FK nếu schema không khai báo.

Các bảng ngữ cảnh: không có.

```mermaid
erDiagram
    sim_clock {
        bool id PK
        timestamptz anchor_real
        timestamptz anchor_sim
        float8 speed
    }
    idempotency_keys {
        varchar key PK
        varchar endpoint
        varchar request_hash
        int4 status_code "nullable"
        jsonb response "nullable"
        timestamptz created_at
    }
    cdc_heartbeat {
        int4 id PK
        timestamptz ts "nullable"
    }
    debezium_signals {
        varchar id PK
        varchar type
        text data "nullable"
    }
    outbox_events {
        uuid id PK
        varchar aggregatetype
        varchar aggregateid
        varchar type
        jsonb payload
    }
```
