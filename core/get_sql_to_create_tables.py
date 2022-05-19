
def get_sql(schema: str) -> str:
    if schema == 'raw':
        create_tables = f"""

            SET datestyle = dmy;

            CREATE TABLE IF NOT EXISTS {schema}.product (
                product_id INTEGER,
                product_name TEXT NOT NULL, 
                product_category TEXT,
                product_type TEXT,
                recurrent TEXT,
                cost_for_call DECIMAL,
                cost_for_sms DECIMAL,
                cost_for_data DECIMAL,
                allowance_sms TEXT,
                allowance_voice TEXT,
                allowance_data TEXT,
                price DECIMAL,
                date_of_upload TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS {schema}.customer (
                customer_id INTEGER,
                first_name TEXT NOT NULL, 
                last_name TEXT NOT NULL,
                gender TEXT NOT NULL,
                language TEXT, 
                agree_for_promo INTEGER,
                autopay_card TEXT,
                customer_category TEXT, 
                status TEXT,
                data_of_birth DATE,
                customer_since TEXT, 
                email TEXT,
                region TEXT,
                termination_date DATE, 
                msisdn TEXT,
                date_of_upload TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS {schema}.product_instance (
                product_instance_id_pk INTEGER,
                customer_id_fk INTEGER NOT NULL, 
                product_id_fk INTEGER NOT NULL,
                activation_date DATE,
                termination_date DATE, 
                status INTEGER,
                distribution_channel TEXT,
                date_of_upload TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS {schema}.costed_event (
                event_id_pk INTEGER,
                product_instance_id_fk INTEGER NOT NULL, 
                calling_msisdn INTEGER,
                called_msisdn INTEGER,
                date TIMESTAMP, 
                cost DECIMAL,
                duration INTEGER,
                number_of_sms INTEGER, 
                number_of_data INTEGER,
                event_type TEXT,
                direction TEXT, 
                roaming INTEGER,
                date_of_upload TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS {schema}.charge (
                charge_id_pk INTEGER,
                product_instance_id_fk INTEGER NOT NULL, 
                charge_counter INTEGER,
                date TIMESTAMP,
                cost DECIMAL, 
                event_type BOOLEAN,
                date_of_upload TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS {schema}.payment (
                payment_id_pk INTEGER,
                customer_id_fk INTEGER NOT NULL, 
                payment_method TEXT,
                date TIMESTAMP,
                amount DECIMAL,
                date_of_upload TIMESTAMP
            );
        """
    else:
        create_tables = f"""

            SET datestyle = dmy;

            CREATE TABLE IF NOT EXISTS {schema}.product (
                product_id SERIAL PRIMARY KEY,
                product_name TEXT NOT NULL, 
                product_category TEXT,
                product_type TEXT,
                recurrent TEXT,
                cost_for_call DECIMAL,
                cost_for_sms DECIMAL,
                cost_for_data DECIMAL,
                allowance_sms TEXT,
                allowance_voice TEXT,
                allowance_data TEXT,
                price DECIMAL
            );
            CREATE TABLE IF NOT EXISTS {schema}.customer (
                customer_id SERIAL PRIMARY KEY,
                first_name TEXT NOT NULL, 
                last_name TEXT NOT NULL,
                gender TEXT NOT NULL,
                language TEXT, 
                agree_for_promo INTEGER,
                autopay_card TEXT,
                customer_category TEXT, 
                status TEXT,
                data_of_birth DATE,
                customer_since TEXT, 
                email TEXT,
                region TEXT,
                termination_date DATE, 
                msisdn TEXT
            );
            CREATE TABLE IF NOT EXISTS {schema}.product_instance (
                product_instance_id_pk SERIAL PRIMARY KEY,
                customer_id_fk INTEGER REFERENCES customer(customer_id) NOT NULL, 
                product_id_fk INTEGER REFERENCES product(product_id) NOT NULL,
                activation_date DATE,
                termination_date DATE, 
                status INTEGER,
                distribution_channel TEXT
            );
            CREATE TABLE IF NOT EXISTS {schema}.costed_event (
                event_id_pk SERIAL PRIMARY KEY,
                product_instance_id_fk INTEGER REFERENCES product_instance(product_instance_id_pk) NOT NULL, 
                calling_msisdn INTEGER,
                called_msisdn INTEGER,
                date TIMESTAMP, 
                cost DECIMAL,
                duration INTEGER,
                number_of_sms INTEGER, 
                number_of_data INTEGER,
                event_type TEXT,
                direction TEXT, 
                roaming INTEGER
            );
            CREATE TABLE IF NOT EXISTS {schema}.charge (
                charge_id_pk SERIAL PRIMARY KEY,
                product_instance_id_fk INTEGER REFERENCES product_instance(product_instance_id_pk) NOT NULL, 
                charge_counter INTEGER,
                date TIMESTAMP,
                cost DECIMAL, 
                event_type BOOLEAN
            );
            CREATE TABLE IF NOT EXISTS {schema}.payment (
                payment_id_pk SERIAL PRIMARY KEY,
                customer_id_fk INTEGER REFERENCES customer(customer_id) NOT NULL, 
                payment_method TEXT,
                date TIMESTAMP,
                amount DECIMAL
            );
        """
    return create_tables


