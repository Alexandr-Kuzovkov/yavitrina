CREATE TABLE IF NOT EXISTS category
(
    id bigserial not null constraint category_pk primary key,
    url VARCHAR(255) NOT NULL,
    html TEXT,
    created_at timestamp with time zone default now() not null,
    title VARCHAR(255) NOT NULL,
    parent_id BIGINT,
    parent_url VARCHAR(255)
);

ALTER TABLE category OWNER TO "vitrina";

CREATE TABLE IF NOT EXISTS tag
(
    id serial not null constraint tag_pk primary key,
    url VARCHAR(255) NOT NULL,
    html TEXT,
    created_at timestamp with time zone default now() not null,
    title VARCHAR(255) NOT NULL,
    updated_at timestamp with time zone
);


ALTER TABLE tag OWNER TO "vitrina";

CREATE UNIQUE INDEX uidx_category_url on category (url);
CREATE UNIQUE INDEX uidx_tag_title on tag (title);

CREATE TABLE IF NOT EXISTS image
(
    id bigserial not null constraint image_pk primary key,
    url VARCHAR(255) NOT NULL,
    path VARCHAR(255) NOT NULL,
    created_at timestamp with time zone default now() not null,
    product_id VARCHAR(255) NOT NULL,
    filename VARCHAR(255) NOT NULL
);

ALTER TABLE image OWNER TO "vitrina";

CREATE TABLE IF NOT EXISTS product_card
(
    id bigserial not null constraint product_card_pk primary key,
    url VARCHAR(255) NOT NULL,
    html TEXT,
    created_at timestamp with time zone default now() not null,
    title VARCHAR(255) NOT NULL,
    page VARCHAR(255) NOT NULL,
    price NUMERIC(15,2),
    product_id VARCHAR(255) NOT NULL,
    img VARCHAR(255) NOT NULL
);

ALTER TABLE product_card OWNER TO "vitrina";
CREATE UNIQUE INDEX uidx_product_card_product_id on product_card (product_id);

CREATE TABLE IF NOT EXISTS product
(
    id bigserial not null constraint product_pk primary key,
    url VARCHAR(255) NOT NULL,
    html TEXT,
    created_at timestamp with time zone default now() not null,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    price NUMERIC(15,2),
    product_id VARCHAR(255) NOT NULL,
    category VARCHAR(255) NOT NULL,
    category_id BIGINT,
    shop_link VARCHAR(255),
    shop_link2 VARCHAR(255),
    parameters TEXT
);

ALTER TABLE product OWNER TO "vitrina";
CREATE UNIQUE INDEX uidx_product_product_id on product (product_id);
CREATE UNIQUE INDEX uidx_product_url on product (url);





