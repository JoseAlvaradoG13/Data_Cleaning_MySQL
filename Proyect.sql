-- Data Cleaning Proyect


SELECT * 
FROM layoffs;

-- We create a table, so we can add, modify or delete data, with out toching the raw data  

CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT layoffs_staging 
SELECT * FROM layoffs;

-- Delete Duplicates
-- We create a cte table, so we can work with the data

With duplicate_cte As
(
SELECT *,
Row_Number() Over(
Partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) As row_num 
FROM layoffs_staging
)
Select *
From duplicate_cte
Where row_num > 1;

-- We check if the result that the duplicate_cte table is giving us is correct

SELECT * 
FROM layoffs_staging
Where company = 'Casper';

-- Once we check that the data from duplicate_cte is correct, we can start deleting the duplicate data, to do that, what we are going to do is create a new table calls, layoffs_staging2,
-- on this one we are going to create a new column name, row_num

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` Int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- We check if it was created correcly

Select *
From layoffs_staging2;

-- We are going to add the information from layoffs_staging, to layoffs_staging, and also add the information to the row_num, what is going to happen on the row_num, is that each row is going to have a 1 on the row_num but
-- if there is a duplicate row is going to have a 2, and so on.

Insert Into layoffs_staging2
SELECT *,
Row_Number() Over(
Partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) As row_num 
FROM layoffs_staging;
Select *
From layoffs_staging2;

-- We check if there is any row with row_num > 1

Select *
From layoffs_staging2
Where row_num > 1;

-- Now we can delete the duplicate data

DELETE
From layoffs_staging2
Where row_num > 1;

-- Standardizing data

-- There are some words with blanck space on the company column, we can fix that easily with Trim

Select company, Trim(company)
From layoffs_staging2;

-- Now we just need to update it

Update layoffs_staging2
Set company = Trim(company);

-- Let´s check if all the data in industry is correct 

Select Distinct industry
From layoffs_staging2
Order by 1;

-- There is a few data that are the same, but have a different name, like Crypto and CryptoCurrency, so we need to fix it, and there is also a few blank and null cells, but we are going to fix that later
Select *
From layoffs_staging2
Where industry Like 'Crypto%';

-- Now we need to update all the name with the Crypto word into Crypto

Update layoffs_staging2 
Set industry = 'Crypto'
Where industry like 'Crypto%';

-- Now let´s check the location 

Select Distinct location
From layoffs_staging2
Order by 1;

-- I see a few wierd names, like 'DÃ¼sseldorf' but I´m not sure, so I´m bot going to change it

-- Now let´s check the country

Select Distinct country
From layoffs_staging2
Order by 1;

-- There are 2 'United States', so we need to fix it

Select Distinct country, trim(trailing '.' from country)
From layoffs_staging2
Order by 1;

-- with trim and trailing we can delete the . in United States, 
Update layoffs_staging2 
Set country = trim(trailing '.' from country)
Where country like 'United States%';

-- Now lets check the date column, the main problem I see, is that is in text format, and we need to fix  that

Select `date`,
str_to_date(`date`,'%m/%d/%Y')
From layoffs_staging2;

-- Now we can Update it

Update layoffs_staging2
Set `date` = str_to_date(`date`,'%m/%d/%Y');

-- We have not change the date format, so lets change it

Alter table layoffs_staging2
Modify Column `date` Date;

-- now lets start working with the null
-- nulls on Industry

select *
From layoffs_staging2
Where industry is null
or industry = '';

Select*
From layoffs_staging2
where company = 'Airbnb';

-- Airbnb is is blank on the industry column, but there is another Airbnb on the table that is not blank on industry, so we are going to try a "join"

update layoffs_staging2
set industry = null
Where industry = '';

Select *
From layoffs_staging2 t1
Join layoffs_staging2 t2
	on t1.company = t2.company
Where t1.industry is null 
and t2.industry is not null;

Update layoffs_staging2 t1
Join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
Where t1.industry is null
and t2.industry is not null;

-- we fixed the blancks in industry exept for Bally's Interactive, because we dont have more Bally's Interactive rows

SELECT *
FROM layoffs_staging2;

-- I´m not going to change any of the null data on, total_laid_off, percentage_laid_off and funds_raised_millions, because I dont have the tools and data necesary to make that changes
-- I´m going to delete the rows that are missing the data from total_laid_off and percentage_laid_off, becuase I need to know this infrormation for a future proyect
-- and if I left there is just going to be information that I´m not use it

SELECT *
FROM layoffs_staging2
Where percentage_laid_off is null
	and total_laid_off is null;
    
Delete
FROM layoffs_staging2
Where percentage_laid_off is null
	and total_laid_off is null;
    
-- We are going to delete the row_num, because is not giving us any necesary data

Alter table layoffs_staging2
Drop column row_num;

-- We can also delete rows like, location or stage, but I´m going to leave it there just in case
-- that is all the data cleaning that we are going to do on this proyect
-- End of Proyect
