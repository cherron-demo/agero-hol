use role accountadmin;
create or replace database hol;

CREATE OR REPLACE TABLE cvs_sales (
    sales_date TIMESTAMP_NTZ,
    revenue DECIMAL(10, 2),
    day_of_week STRING,
    city STRING
);

CREATE or replace TABLE app_reviews (
    date_created DATE,
    author_id STRING,
    review STRING
);

INSERT INTO app_reviews (date_created, author_id, review)
VALUES
    ('2024-08-01', '001', 'I recently lost my health insurance so I was a little wary going to use my Single Care card for the first time at my pharmacy this morning. Well , I needn’t have worried. They accepted the card , entered the information and quoted me the exact discounted price as indicated in the Single Care app. It was so easy and I was so relieved. I had to pay just 10% of the cost. Saving $280 !
I’m a happy camper'),
    ('2024-08-02', '002', 'I signed up as a member and received an email stating my medication would be $28.85. This included the $3.00 sign up bonus. This was the price to use at Walgreens. I ran out within 2 hours of this email and was told the price with the info keyed in from my email was $43.45!!! The cashier tried again just to make sure it was keyed in properly. I am disappointed and disgusted! Thanks for nothing SingleCare!'),
    ('2024-08-03', '003', 'SingleCare has been a lifesaver for me! I’ve saved so much money on my prescriptions, and the process is incredibly easy. I love how quickly I can find discounts, and the savings are impressive every time. Its made managing my healthcare so much more affordable. Highly recommended!'),
    ('2024-08-04', '004', 'I went to walgreens to pick up my medication thinking i was going to pay singlecare discount, but the pharmacist kept asking for the id card number?? ... There is no id number! and I showed the card to her and she said, they cant apply the cuppon with no id number?? Is it going to work for discounts or not?'),
    ('2024-08-05', '001', 'Single care has been the only reason I have been able to buy the medications I need to endure my day to day activities. One day when I went to pick up one of my prescriptions and didnt have nearly enough money. The young pharmacist at the window said lets try one of these Singlecare cards on the counter. To the shock of both her and I, my total balance after using the Singlecare card was an astonishing saving of 75% off the original total. I was then able to buy my needed medication. Ive been a happy customer of Singlecare ever since.'),
    ('2024-08-06', '002', 'Your price was great for only 1 month then you went up almost 3 x’s for my next month. This was for generic Cyclosporine. For goodness sakes it is just eye drops. I will be looking at other options come my renewal.'),
    ('2024-08-07', '003', 'Thank you SingleCare for your services. My primary healthcare provider Rx prices went up so high that I could not get paid for my medical care. Cancer is no joke. My quarterly payments came down from over $1000 a month to $370 a month. These payments will be with me the rest of my life. This payment is still high, but I can now afford my Rx prescriptions.'),
    ('2024-08-08', '004', 'So, downloaded singlecare app. searched for Ondansetron scrip. said price of $17 went to walgreens gave them all of the info. showed the app price they entered it and said sorry cant get that price. They said it would be $105. So much for singlecare. 1st time trying to use it and it doesnt work. Sad.');

-- Create the diabetes risk dataset table
CREATE OR REPLACE TABLE diabetes_risk_dataset (
    id INT AUTOINCREMENT,
    age INT,
    bmi FLOAT,
    blood_pressure INT,
    glucose_level INT,
    insulin_level INT,
    cholesterol INT,
    family_history INT,
    physical_activity INT,
    smoking_status INT,
    diabetes_risk INT
);

-- Insert sample records into the table
INSERT INTO diabetes_risk_dataset (age, bmi, blood_pressure, glucose_level, insulin_level, cholesterol, family_history, physical_activity, smoking_status, diabetes_risk) VALUES
(25, 22.5, 70, 85, 100, 180, 0, 1, 0, 0),
(45, 28.0, 80, 140, 150, 220, 1, 0, 1, 1),
(60, 30.5, 85, 160, 200, 240, 1, 0, 1, 1),
(32, 26.0, 75, 95, 120, 190, 0, 1, 0, 0),
(50, 31.0, 90, 150, 180, 230, 1, 0, 1, 1),
(28, 24.0, 68, 90, 110, 175, 0, 1, 0, 0),
(55, 29.5, 82, 145, 160, 210, 1, 0, 1, 1),
(30, 23.0, 78, 92, 105, 185, 0, 1, 0, 0),
(40, 27.5, 76, 130, 140, 200, 1, 0, 1, 1),
(35, 25.0, 74, 88, 95, 170, 0, 1, 0, 0),
(26, 23.5, 72, 89, 108, 178, 0, 1, 0, 0),
(42, 29.0, 78, 145, 152, 215, 1, 1, 1, 1),
(38, 26.5, 75, 130, 135, 190, 0, 1, 1, 0),
(31, 27.2, 73, 110, 100, 180, 0, 1, 0, 0),
(48, 30.0, 88, 155, 170, 225, 1, 0, 1, 1),
(54, 32.0, 92, 165, 190, 240, 1, 0, 1, 1),
(29, 24.0, 70, 95, 105, 175, 0, 1, 0, 0),
(57, 28.5, 86, 140, 155, 210, 1, 0, 1, 1),
(36, 25.5, 76, 102, 110, 175, 0, 1, 0, 0),
(39, 27.0, 79, 126, 130, 200, 1, 1, 1, 1),
(44, 29.5, 83, 150, 145, 215, 1, 0, 1, 1),
(20, 22.0, 65, 85, 95, 170, 0, 1, 0, 0),
(22, 24.0, 67, 90, 100, 175, 0, 1, 0, 0),
(30, 25.0, 70, 95, 100, 180, 0, 1, 0, 0),
(34, 27.5, 75, 100, 105, 180, 0, 1, 0, 0),
(43, 28.0, 80, 140, 150, 220, 1, 0, 1, 1),
(46, 29.0, 85, 160, 175, 230, 1, 0, 1, 1),
(31, 26.5, 72, 115, 120, 190, 0, 1, 0, 0),
(58, 31.5, 90, 155, 185, 240, 1, 0, 1, 1),
(26, 24.5, 78, 92, 105, 175, 0, 1, 0, 0),
(40, 27.5, 80, 130, 140, 200, 1, 1, 1, 1),
(53, 28.0, 87, 145, 160, 210, 1, 0, 1, 1),
(25, 22.5, 70, 85, 100, 180, 0, 1, 0, 0),
(60, 30.0, 85, 160, 190, 240, 1, 0, 1, 1),
(29, 24.0, 76, 100, 110, 180, 0, 1, 0, 0),
(34, 28.5, 78, 120, 130, 190, 0, 1, 0, 0),
(48, 31.0, 90, 150, 175, 225, 1, 0, 1, 1),
(52, 29.0, 85, 140, 150, 215, 1, 0, 1, 1),
(35, 26.0, 72, 105, 115, 175, 0, 1, 0, 0),
(58, 30.5, 84, 155, 185, 240, 1, 0, 1, 1),
(39, 28.0, 80, 130, 140, 200, 1, 1, 1, 1),
(45, 29.5, 82, 140, 150, 220, 1, 0, 1, 1),
(32, 27.0, 75, 100, 110, 180, 0, 1, 0, 0),
(21, 22.8, 68, 90, 100, 175, 0, 1, 0, 0),
(30, 26.2, 75, 105, 120, 185, 0, 1, 0, 0),
(47, 29.0, 82, 140, 155, 220, 1, 0, 1, 1),
(55, 31.0, 87, 150, 170, 230, 1, 0, 1, 1),
(40, 27.5, 76, 130, 140, 200, 1, 1, 1, 1),
(38, 26.5, 73, 120, 135, 190, 0, 1, 1, 0),
(28, 24.8, 70, 85, 105, 180, 0, 1, 0, 0),
(50, 30.0, 88, 150, 175, 220, 1, 0, 1, 1),
(36, 25.5, 76, 102, 115, 175, 0, 1, 0, 0),
(39, 27.0, 79, 126, 130, 200, 1, 1, 1, 1),
(41, 29.0, 83, 148, 140, 215, 1, 0, 1, 1),
(20, 22.0, 65, 85, 90, 170, 0, 1, 0, 0),
(22, 24.0, 67, 90, 100, 175, 0, 1, 0, 0),
(30, 25.0, 70, 95, 100, 180, 0, 1, 0, 0),
(34, 27.5, 75, 100, 105, 180, 0, 1, 0, 0),
(23, 23.0, 65, 89, 100, 180, 0, 1, 0, 0),
(41, 27.5, 75, 120, 135, 190, 1, 1, 1, 1),
(33, 26.0, 70, 95, 110, 185, 0, 1, 0, 0),
(36, 29.5, 80, 130, 150, 200, 0, 1, 0, 0),
(50, 30.0, 88, 140, 155, 215, 1, 0, 1, 1),
(29, 25.5, 72, 100, 120, 170, 0, 1, 0, 0),
(45, 28.0, 82, 130, 140, 210, 1, 0, 1, 1),
(54, 31.0, 90, 150, 160, 240, 1, 0, 1, 1),
(60, 32.5, 95, 160, 180, 250, 1, 0, 1, 1),
(27, 24.5, 70, 88, 102, 180, 0, 1, 0, 0),
(35, 26.0, 75, 110, 115, 185, 0, 1, 0, 0),
(42, 28.0, 80, 140, 150, 200, 1, 0, 1, 1),
(38, 27.5, 78, 135, 145, 210, 0, 1, 0, 0),
(30, 25.5, 76, 90, 100, 170, 0, 1, 0, 0),
(48, 29.0, 88, 145, 160, 220, 1, 0, 1, 1),
(20, 22.5, 66, 86, 99, 175, 0, 1, 0, 0),
(26, 24.0, 70, 90, 110, 180, 0, 1, 0, 0),
(32, 27.5, 75, 100, 120, 185, 0, 1, 0, 0),
(39, 28.0, 80, 140, 150, 200, 1, 0, 1, 1),
(55, 31.5, 85, 160, 175, 225, 1, 0, 1, 1),
(50, 30.0, 87, 150, 170, 220, 1, 0, 1, 1),
(45, 29.5, 82, 140, 155, 215, 1, 0, 1, 1),
(40, 28.0, 80, 130, 140, 200, 0, 1, 0, 0),
(35, 26.5, 74, 110, 120, 185, 0, 1, 0, 0),
(33, 24.5, 72, 95, 110, 175, 0, 1, 0, 0),
(58, 30.0, 88, 155, 185, 240, 1, 0, 1, 1),
(43, 29.0, 85, 140, 150, 220, 1, 0, 1, 1),
(38, 27.0, 78, 130, 140, 205, 0, 1, 1, 0),
(31, 25.0, 76, 100, 105, 180, 0, 1, 0, 0),
(22, 23.0, 68, 85, 95, 170, 0, 1, 0, 0),
(25, 24.5, 72, 90, 100, 175, 0, 1, 0, 0),
(46, 30.5, 82, 150, 170, 210, 1, 0, 1, 1),
(37, 28.5, 80, 135, 150, 195, 0, 1, 0, 0),
(44, 29.0, 83, 140, 155, 220, 1, 0, 1, 1),
(28, 25.0, 70, 95, 110, 180, 0, 1, 0, 0),
(52, 30.0, 88, 150, 175, 225, 1, 0, 1, 1),
(54, 31.5, 90, 160, 185, 240, 1, 0, 1, 1),
(48, 29.0, 85, 145, 165, 210, 1, 0, 1, 1),
(33, 25.0, 74, 102, 115, 180, 0, 1, 0, 0),
(36, 26.5, 76, 120, 125, 185, 0, 1, 0, 0),
(49, 30.0, 82, 145, 150, 220, 1, 0, 1, 1),
(51, 31.0, 88, 150, 160, 210, 1, 0, 1, 1),
(29, 24.0, 75, 92, 108, 175, 0, 1, 0, 0),
(40, 27.5, 78, 130, 145, 200, 1, 0, 1, 1),
(34, 25.0, 74, 100, 110, 180, 0, 1, 0, 0),
(30, 26.0, 75, 95, 100, 175, 0, 1, 0, 0),
(22, 24.0, 70, 88, 100, 180, 0, 1, 0, 0),
(27, 25.0, 72, 95, 115, 175, 0, 1, 0, 0),
(53, 29.5, 85, 155, 165, 230, 1, 0, 1, 1),
(60, 31.0, 90, 160, 180, 240, 1, 0, 1, 1),
(35, 26.0, 76, 110, 125, 185, 0, 1, 0, 0),
(45, 29.0, 82, 130, 150, 220, 1, 1, 1, 1),
(48, 30.0, 88, 145, 155, 225, 1, 0, 1, 1),
(29, 25.0, 74, 90, 100, 170, 0, 1, 0, 0),
(32, 28.0, 75, 95, 105, 180, 0, 1, 0, 0),
(38, 26.5, 78, 140, 145, 210, 1, 0, 1, 1),
(55, 30.5, 85, 150, 175, 240, 1, 0, 1, 1),
(39, 27.0, 80, 120, 130, 195, 0, 1, 0, 0),
(52, 29.0, 82, 135, 150, 220, 1, 0, 1, 1),
(20, 22.0, 65, 85, 90, 170, 0, 1, 0, 0),
(24, 23.5, 68, 88, 95, 175, 0, 1, 0, 0),
(33, 25.5, 74, 100, 110, 180, 0, 1, 0, 0),
(50, 31.0, 90, 145, 155, 230, 1, 0, 1, 1),
(42, 28.5, 80, 130, 140, 200, 1, 0, 1, 1),
(37, 27.5, 76, 115, 125, 185, 0, 1, 0, 0),
(46, 29.0, 84, 135, 150, 210, 1, 0, 1, 1),
(54, 30.0, 90, 150, 160, 225, 1, 0, 1, 1),
(49, 30.0, 88, 140, 175, 220, 1, 0, 1, 1),
(41, 29.0, 82, 130, 150, 200, 1, 1, 1, 1),
(31, 25.5, 75, 95, 105, 180, 0, 1, 0, 0),
(29, 24.0, 70, 88, 95, 175, 0, 1, 0, 0),
(26, 23.5, 72, 90, 100, 180, 0, 1, 0, 0),
(35, 27.0, 76, 100, 120, 185, 0, 1, 0, 0),
(33, 25.5, 74, 102, 115, 170, 0, 1, 0, 0),
(44, 29.5, 82, 135, 140, 215, 1, 0, 1, 1),
(58, 30.0, 90, 150, 160, 225, 1, 0, 1, 1),
(48, 28.5, 85, 140, 150, 200, 1, 0, 1, 1),
(37, 27.5, 78, 130, 135, 190, 0, 1, 1, 0),
(39, 27.0, 75, 120, 130, 195, 0, 1, 0, 0),
(32, 25.0, 74, 100, 110, 180, 0, 1, 0, 0),
(25, 24.0, 72, 95, 100, 175, 0, 1, 0, 0),
(42, 29.0, 80, 135, 145, 200, 1, 0, 1, 1),
(20, 22.5, 66, 86, 95, 170, 0, 1, 0, 0);

select * from diabetes_risk_dataset;

CREATE OR REPLACE PROCEDURE insert_cvs_sales_data()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var start_date = new Date();
start_date.setFullYear(start_date.getFullYear() - 1); // One year ago
var end_date = new Date();
var cities = ['Manhattan', 'Queens', 'Brooklyn'];

var current_date = start_date;

// Create an array to accumulate the rows for batch insertion
var rows = [];

while (current_date <= end_date) {
    // Format the current date as YYYY-MM-DD
    var sales_date = current_date.toISOString().split('T')[0];

    // Determine the day of the week
    var options = { weekday: 'long' };
    var day_of_week = current_date.toLocaleDateString('en-US', options).trim();

    // Generate rows for all cities on this date
    for (var i = 0; i < cities.length; i++) {
        var city = cities[i];

        // Generate random revenue between 1000 and 5000
        var revenue = Math.round(Math.random() * 4000 + 1000);

        // Accumulate the row for batch insertion
        rows.push(`('${sales_date}', ${revenue}, '${day_of_week}', '${city}')`);
    }

    // Move to the next day
    current_date.setDate(current_date.getDate() + 1);
}

// Construct a single large SQL insert statement for all rows
var sql_command = `INSERT INTO cvs_sales (sales_date, revenue, day_of_week, city) VALUES ` + rows.join(", ");

// Execute the batch insert
var statement1 = snowflake.createStatement({ sqlText: sql_command });
statement1.execute();

return 'Data inserted successfully!';
$$;

call insert_cvs_sales_data();

select * from cvs_sales;
select * from app_reviews;
select * from diabetes_risk_dataset;

