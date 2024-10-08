create or replace TABLE CUSTOMER_3.STAGE_SCHEMA.CUSTOMER_STAGE 
   (
    CustomerID INT PRIMARY KEY,
    FirstName STRING,
    LastName STRING,
    Email STRING,
    Phone STRING,
    Address STRING,
    City STRING,
    State STRING,
    ZipCode STRING,
    Country STRING
   );

  CREATE OR REPLACE TABLE CUSTOMER_3.TARGET_SCHEMA.TARGET_CUSTOMER (
    Cus_ID INT PRIMARY KEY,
    First_Name STRING,
    Last_Name STRING,
    e_mail STRING,
    P_hone STRING,
    A_ddress STRING,
    City STRING,
    State STRING,
    ZipCode STRING,
    Country STRING,
    Insert_Date_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    Last_Update_Date_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

 CREATE OR REPLACE STREAM CUSTOMER_3.STAGE_SCHEMA.CUS_STREAM
ON TABLE CUSTOMER_3.STAGE_SCHEMA.CUSTOMER_STAGE
APPEND_ONLY = FALSE;  -- Ensure the stream captures both inserts and updates

 MERGE INTO CUSTOMER_3.TARGET_SCHEMA.TARGET_CUSTOMER TGT
USING CUSTOMER_3.STAGE_SCHEMA.CUS_STREAM STR  -- Use the stream instead of the stage table
ON TGT.Cus_ID = STR.CustomerID  -- Match records based on CustomerID
WHEN MATCHED
    AND STR.METADATA$ACTION IN ('INSERT', 'UPDATE')  -- Check if the action is 'INSERT' or 'UPDATE'
    AND (TGT.First_Name <> STR.FirstName
        OR TGT.Last_Name <> STR.LastName
        OR TGT.e_mail <> STR.Email
        OR TGT.P_hone <> STR.Phone
        OR TGT.A_ddress <> STR.Address
        OR TGT.City <> STR.City
        OR TGT.State <> STR.State
        OR TGT.ZipCode <> STR.ZipCode
        OR TGT.Country <> STR.Country)
THEN 
    UPDATE SET
        TGT.First_Name = STR.FirstName,
        TGT.Last_Name = STR.LastName,
        TGT.e_mail = STR.Email,
        TGT.P_hone = STR.Phone,
        TGT.A_ddress = STR.Address,
        TGT.City = STR.City,
        TGT.State = STR.State,
        TGT.ZipCode = STR.ZipCode,
        TGT.Country = STR.Country,
        TGT.Last_Update_Date_ts = CURRENT_TIMESTAMP  -- Update the last updated timestamp
WHEN NOT MATCHED AND STR.METADATA$ACTION = 'INSERT' THEN
INSERT (
    Cus_ID,
    First_Name,
    Last_Name,
    e_mail,
    P_hone,
    A_ddress,
    City,
    State,
    ZipCode,
    Country,
    Insert_Date_ts,
    Last_Update_Date_ts
) VALUES (
    STR.CustomerID,
    STR.FirstName,
    STR.LastName,
    STR.Email,
    STR.Phone,
    STR.Address,
    STR.City,
    STR.State,
    STR.ZipCode,
    STR.Country,
    CURRENT_TIMESTAMP,  -- Set the timestamp when the record was inserted
    CURRENT_TIMESTAMP   -- Set the timestamp when the record was last updated
);

CREATE OR REPLACE TASK CUSTOMER_3.STAGE_SCHEMA.TASK_CUSTOMER_DATA_LOAD
SCHEDULE = '1 MINUTES'
WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER_3.STAGE_SCHEMA.CUS_STREAM')
AS
MERGE INTO CUSTOMER_3.TARGET_SCHEMA.TARGET_CUSTOMER TGT
USING CUSTOMER_3.STAGE_SCHEMA.CUS_STREAM STR  -- Use the stream instead of the stage table
ON TGT.Cus_ID = STR.CustomerID  -- Match records based on CustomerID
WHEN MATCHED
    AND STR.METADATA$ACTION IN ('INSERT', 'UPDATE')  -- Check if the action is 'INSERT' or 'UPDATE'
    AND (TGT.First_Name <> STR.FirstName
        OR TGT.Last_Name <> STR.LastName
        OR TGT.e_mail <> STR.Email
        OR TGT.P_hone <> STR.Phone
        OR TGT.A_ddress <> STR.Address
        OR TGT.City <> STR.City
        OR TGT.State <> STR.State
        OR TGT.ZipCode <> STR.ZipCode
        OR TGT.Country <> STR.Country)
THEN 
    UPDATE SET
        TGT.First_Name = STR.FirstName,
        TGT.Last_Name = STR.LastName,
        TGT.e_mail = STR.Email,
        TGT.P_hone = STR.Phone,
        TGT.A_ddress = STR.Address,
        TGT.City = STR.City,
        TGT.State = STR.State,
        TGT.ZipCode = STR.ZipCode,
        TGT.Country = STR.Country,
        TGT.Last_Update_Date_ts = CURRENT_TIMESTAMP  -- Update the last updated timestamp
WHEN NOT MATCHED AND STR.METADATA$ACTION = 'INSERT' THEN
INSERT (
    Cus_ID,
    First_Name,
    Last_Name,
    e_mail,
    P_hone,
    A_ddress,
    City,
    State,
    ZipCode,
    Country,
    Insert_Date_ts,
    Last_Update_Date_ts
) VALUES (
    STR.CustomerID,
    STR.FirstName,
    STR.LastName,
    STR.Email,
    STR.Phone,
    STR.Address,
    STR.City,
    STR.State,
    STR.ZipCode,
    STR.Country,
    CURRENT_TIMESTAMP,  -- Set the timestamp when the record was inserted
    CURRENT_TIMESTAMP   -- Set the timestamp when the record was last updated
);

  SHOW TASKS LIKE 'TASK_CUSTOMER_DATA_LOAD';
  ALTER TASK CUSTOMER_3.STAGE_SCHEMA.TASK_CUSTOMER_DATA_LOAD RESUME;


  
 INSERT INTO CUSTOMER_3.STAGE_SCHEMA.CUSTOMER_STAGE (CustomerID, FirstName, LastName, Email, Phone, Address, City, State, ZipCode, Country) VALUES

(1, 'John', 'Doe', 'john.doe@example.com', '555-1234', '123 Elm St', 'Springfield', 'IL', '62701', 'USA'),
(2, 'Jane', 'Smith', 'jane.smith@example.com', '555-5678', '456 Oak St', 'Springfield', 'IL', '62702', 'USA'),
(3, 'Mike', 'Johnson', 'mike.johnson@example.com', '555-8765', '789 Pine St', 'Springfield', 'IL', '62703', 'USA'),
(4, 'Emily', 'Davis', 'emily.davis@example.com', '555-4321', '101 Maple St', 'Springfield', 'IL', '62704', 'USA'),
(5, 'Chris', 'Brown', 'chris.brown@example.com', '555-1357', '202 Cedar St', 'Springfield', 'IL', '62705', 'USA'),
(6, 'Anna', 'Wilson', 'anna.wilson@example.com', '555-2468', '303 Birch St', 'Springfield', 'IL', '62706', 'USA'),
(7, 'James', 'Taylor', 'james.taylor@example.com', '555-3698', '404 Walnut St', 'Springfield', 'IL', '62707', 'USA'),
(8, 'Laura', 'Anderson', 'laura.anderson@example.com', '555-1472', '505 Ash St', 'Springfield', 'IL', '62708', 'USA'),
(9, 'David', 'Martinez', 'david.martinez@example.com', '555-2583', '606 Maplewood St', 'Springfield', 'IL', '62709', 'USA'),
(10, 'Sophia', 'Hernandez', 'sophia.hernandez@example.com', '555-3695', '707 Willow St', 'Springfield', 'IL', '62710', 'USA');



 SELECT * FROM  CUSTOMER_3.TARGET_SCHEMA.TARGET_CUSTOMER;

 INSERT INTO CUSTOMER_3.STAGE_SCHEMA.CUSTOMER_STAGE (CustomerID, FirstName, LastName, Email, Phone, Address, City, State, ZipCode, Country) VALUES

(1, 'Manohar', 'Gadige', 'john.doe@example.com', '555-1234', '123 Elm St', 'Springfield', 'IL', '62701', 'india'),
(2, 'Ajay', 'Anitha', 'jane.smith@example.com', '555-5678', '456 Oak St', 'Springfield', 'IL', '62702', 'Ppakisthan'),
(3, 'Sunil', 'Army', 'mike.johnson@example.com', '555-8765', '789 Pine St', 'Springfield', 'IL', '62703', 'mumbai'),
(4, 'Emily', 'Davis', 'emily.davis@example.com', '555-4321', '101 Maple St', 'Springfield', 'IL', '62704', 'USA'),
(5, 'Chris', 'Brown', 'chris.brown@example.com', '555-1357', '202 Cedar St', 'Springfield', 'IL', '62705', 'USA'),
(6, 'Anna', 'Wilson', 'anna.wilson@example.com', '555-2468', '303 Birch St', 'Springfield', 'IL', '62706', 'USA'),
(7, 'James', 'Taylor', 'james.taylor@example.com', '555-3698', '404 Walnut St', 'Springfield', 'IL', '62707', 'USA'),
(8, 'Laura', 'Anderson', 'laura.anderson@example.com', '555-1472', '505 Ash St', 'Springfield', 'IL', '62708', 'USA'),
(9, 'David', 'Martinez', 'david.martinez@example.com', '555-2583', '606 Maplewood St', 'Springfield', 'IL', '62709', 'USA'),
(10, 'Sophia', 'Hernandez', 'sophia.hernandez@example.com', '555-3695', '707 Willow St', 'Springfield', 'IL', '62710', 'USA');

SELECT * FROM  CUSTOMER_3.TARGET_SCHEMA.TARGET_CUSTOMER;

SELECT  LAST_UPDATE_DATE_TS from CUSTOMER_3.TARGET_SCHEMA.TARGET_CUSTOMER 
WHERE  LAST_UPDATE_DATE_TS ='2024-09-30 04:31:04.496'
ORDER BY LAST_UPDATE_DATE_TS DESC; 