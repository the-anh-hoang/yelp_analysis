CREATE DATABASE IF NOT EXISTS yelp_data;
USE yelp_data;

-- =============================
-- Core Business Tables
-- =============================

CREATE TABLE Business (
    business_id VARCHAR(22) PRIMARY KEY,
    name VARCHAR(255),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(10),
    postal_code VARCHAR(20),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    stars FLOAT,
    review_count INT,
    is_open BOOLEAN,
    categories TEXT
);

CREATE TABLE OpenHour (
    business_id VARCHAR(22) PRIMARY KEY,
    monday VARCHAR(20),
    tuesday VARCHAR(20),
    wednesday VARCHAR(20),
    thursday VARCHAR(20),
    friday VARCHAR(20),
    saturday VARCHAR(20),
    sunday VARCHAR(20),
    FOREIGN KEY (business_id) REFERENCES Business(business_id)
);

-- =============================
-- Shared Attributes (used across all ISA types)
-- =============================

CREATE TABLE AttributesCommon (
    business_id VARCHAR(22) PRIMARY KEY,
    accepts_credit_cards BOOLEAN,
    accepts_bitcoin BOOLEAN,
    by_appointment_only BOOLEAN,
    wifi ENUM('no', 'free', 'paid'),
    bike_parking BOOLEAN, 
    wheel_chair_accessible BOOLEAN,
    parking_garage BOOLEAN,
    parking_street BOOLEAN,
    parking_validated BOOLEAN,
    parking_lot BOOLEAN,
    parking_valet BOOLEAN,
    FOREIGN KEY (business_id) REFERENCES Business(business_id)
);

-- =============================
-- ISA Tables (extend Business)
-- =============================

CREATE TABLE AttributesAutomotive (
    business_id VARCHAR(22) PRIMARY KEY,
    FOREIGN KEY (business_id) REFERENCES Business(business_id)
);

CREATE TABLE AttributesHealthcare (
    business_id VARCHAR(22) PRIMARY KEY,
    accepts_insurance BOOLEAN,
    FOREIGN KEY (business_id) REFERENCES Business(business_id)
);

CREATE TABLE AttributesHospitality (
    business_id VARCHAR(22) PRIMARY KEY,
    price_range INT,
    alcohol ENUM('none', 'beer_and_wine', 'full_bar'),
    noise_level ENUM('quiet', 'average', 'loud', 'very loud'),
    smoking ENUM('no','yes', 'outdoor'),
    attire ENUM('casual', 'dressy', 'formal'),
    reservations BOOLEAN,
    good_for_groups BOOLEAN,
    delivery BOOLEAN,
    cater BOOLEAN,
    dogs_allowed BOOLEAN,
    ambience JSON,
    good_for_meal JSON,
    music JSON,
    FOREIGN KEY (business_id) REFERENCES Business(business_id)
);

CREATE TABLE AttributesNightlife (
    business_id VARCHAR(22) PRIMARY KEY,
    alcohol ENUM('none', 'beer_and_wine', 'full_bar'),
    ambience JSON,
    best_nights JSON,
    good_for_meal JSON,
    noise_level ENUM('quiet', 'average', 'loud', 'very loud'),
    smoking ENUM('no', 'yes', 'outdoor'),
    restaurant_price_range INT,
    restaurant_attire TEXT,
    restaurant_reservation BOOLEAN,
    restaurant_delivery BOOLEAN,
    restaurant_table_service BOOLEAN,
    happy_hour BOOLEAN,
    coat_check BOOLEAN,
    good_for_dancing BOOLEAN,
    caters BOOLEAN,
    has_tv BOOLEAN,
    dogs_allowed BOOLEAN, 
    FOREIGN KEY (business_id) REFERENCES Business(business_id)
);

CREATE TABLE AttributesPersonalCare (
    business_id VARCHAR(22) PRIMARY KEY,
    accepts_insurance BOOLEAN,
    hair_specializes_in JSON,
    FOREIGN KEY (business_id) REFERENCES Business(business_id)
);

CREATE TABLE AttributesRetail (
    business_id VARCHAR(22) PRIMARY KEY,
    caters BOOLEAN,
    outdoor_seating BOOLEAN,
    reservations BOOLEAN,
    drive_through BOOLEAN,
    has_tv BOOLEAN,
    table_service BOOLEAN,
    alcohol ENUM('none', 'beer_and_wine', 'full_bar'),
    coat_check BOOLEAN,
    noise_level ENUM('quiet', 'average', 'loud', 'very loud'),
    good_for_groups BOOLEAN,
    attire ENUM('casual', 'dressy', 'formal'),
    ambience JSON,
    smoking ENUM('no','yes', 'outdoor'),
    accepts_insurance BOOLEAN,
    open_24_hours BOOLEAN,
    music JSON,
    good_for_dancing BOOLEAN,
    FOREIGN KEY (business_id) REFERENCES Business(business_id)
);

CREATE TABLE AttributesRestaurant (
    business_id VARCHAR(22) PRIMARY KEY,
    price_range INT,
    alcohol ENUM('none', 'beer_and_wine', 'full_bar'),
    noise_level ENUM('quiet', 'average', 'loud', 'very_loud'),
    smoking ENUM('no', 'yes', 'outdoor'),
    attire TEXT,
    restaurant_takeout BOOLEAN,
    restaurant_delivery BOOLEAN,
    restaurant_reservations BOOLEAN,
    restaurant_good_for_groups BOOLEAN,
    restaurant_table_service BOOLEAN,
    restaurant_counter_service BOOLEAN,
    caters BOOLEAN,
    has_tv BOOLEAN,
    drive_thru BOOLEAN,
    happy_hour BOOLEAN,
    dogs_allowed BOOLEAN,
    coat_check BOOLEAN,
    good_for_dancing BOOLEAN,
    byob BOOLEAN,
    corkage BOOLEAN,
    open_24_hours BOOLEAN,
    ambience JSON,
    music JSON,
    good_for_meal JSON,
    best_nights JSON,
    byo_b_corkage TEXT,
    ages_allowed ENUM('allages', '21plus', '18plus'),
    dietary_restrictions JSON,
    FOREIGN KEY (business_id) REFERENCES Business(business_id)
);
