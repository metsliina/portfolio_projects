-- Analysis Project: "COVID-19 Rates and Insights"
-- Problem: Understand trends in cases and deaths.
-- Solution: Key findings from aggregated and analyzed data.
-- Outcome: Identified countries with the highest infection and death rates, 
-- highlighting disparities in public health responses.
-- Technologies: MySQL. 
-- The resulting data was visualized in a separate Tableau Dashboard.
-- Data Source: ourworldindata.org/covid-deaths
-- Time Range of Dataset: 01.01.2020 - 29.04.2021.

-- Inspecting dataset
SELECT *
FROM portfolioproject.deaths
ORDER BY 3,4

-- Selecting data we are going to use 
SELECT 
	location, 
    date, 
    total_cases, 
    new_cases, 
    total_deaths, 
    population
FROM portfolioproject.deaths
ORDER BY 1 ,2 

-- Looking at total cases vs total deaths in Estonia
-- Shows likelihood of dying if you contract Covid in Estonia
SELECT 
	location, 
    date, 
    total_cases, 
    total_deaths, 
    (total_deaths/total_cases)*100 AS death_pct
FROM portfolioproject.deaths
WHERE location LIKE 'Estonia'
ORDER BY 1,2


-- Looking at total cases vs population in Estonia
-- Shows what percentage of population got infected with Covid-19
SELECT 
	location, 
    date, 
    population, 
    total_cases, 
    (total_cases/population)*100 AS pct_population_infected
FROM portfolioproject.deaths
WHERE location LIKE 'Estonia'
ORDER BY 2 

-- Looking at countries with highest infection rate compared to population
-- Filter out rows where the continent column is either NULL or contains only whitespace.
-- This ensures that only country-level data is included, as some rows contain 
-- continent names in the 'location' column but leave the 'continent' column blank.
-- By excluding these rows, we avoid duplicate or misleading entries in the results.
SELECT 
	location, 
    population, 
    MAX(total_cases) as highest_infection_count, 
    MAX((total_cases/population))*100 AS pct_population_infected
FROM portfolioproject.deaths
WHERE continent IS NOT NULL 
	AND TRIM(continent) != ''
GROUP BY location, population
ORDER BY pct_population_infected DESC

-- Looking at countries with the highest death count per population
SELECT 
	location, 
    population,
    MAX(total_deaths) as total_death_count,
    MAX((total_deaths/population))*100 AS pct_population_deaths
FROM portfolioproject.deaths
WHERE continent IS NOT NULL 
	AND TRIM(continent) != ''
GROUP BY location, population
ORDER BY pct_population_deaths DESC

-- Let's break things down by continent
-- Showing the continents with the highest death count
SELECT 
	continent, 
    MAX(total_deaths) as total_death_count
FROM portfolioproject.deaths
WHERE continent IS NOT NULL 
	AND TRIM(continent) != ''
GROUP BY continent
ORDER BY total_death_count DESC


-- Looking at global totals

SELECT 
    SUM(new_cases) AS total_cases, 
    SUM(new_deaths) AS total_deaths,
    SUM(new_deaths)/SUM(new_cases)*100 AS death_pct
FROM portfolioproject.deaths
WHERE continent IS NOT NULL 
	AND TRIM(continent) != ''
ORDER BY 1,2

-- Moving on to vaccinations table ('vacc')and joining the two tables together
-- Inspecting imported data for vacc table
SELECT *
FROM portfolioproject.vacc
ORDER BY 3,4

-- Joining the two tables together
SELECT *
FROM portfolioproject.deaths AS de
JOIN portfolioproject.vacc AS va
	ON de.location=va.location AND de.date=va.date
    
-- Looking at total population vs vaccinations
-- Using CTE to use aggregated data
With Popul_vs_vaccin  (Continent, Location, Date, Population, new_vaccinations, rolling_people_vaccinated)
AS (
SELECT 
	de.continent, 
    de.location, 
    de.date, 
    de.population, 
    va.new_vaccinations,
    SUM(va.new_vaccinations) OVER (PARTITION BY de.location ORDER BY de.location,de.date) AS rolling_people_vaccinated
FROM portfolioproject.deaths AS de
JOIN portfolioproject.vacc AS va
	ON de.location=va.location AND de.date=va.date
WHERE de.continent IS NOT NULL 
	AND TRIM(de.continent) != ''
)
-- This query calculates the percentage of vaccinated populations globally.
SELECT 
	*, 
    (rolling_people_vaccinated/population)*100 AS rolling_pct_vaccinated_population
FROM Popul_vs_vaccin


-- Creating TEMPORARY TABLE
DROP TEMPORARY TABLE IF EXISTS pct_population_vaccinated;
CREATE TEMPORARY TABLE pct_population_vaccinated
(
    continent VARCHAR(255),
    location VARCHAR(255),
    date DATETIME,
    population INTEGER,
    new_vaccinations INTEGER, 
    rolling_people_vaccinated INTEGER 
);
INSERT INTO pct_population_vaccinated (continent, location, date, population, new_vaccinations, rolling_people_vaccinated)
SELECT 
    de.continent, 
    de.location, 
    de.date, 
    de.population, 
    va.new_vaccinations,
    SUM(va.new_vaccinations) OVER (PARTITION BY de.location ORDER BY de.location, de.date) AS rolling_people_vaccinated
FROM portfolioproject.deaths AS de
JOIN portfolioproject.vacc AS va
    ON de.location = va.location AND de.date = va.date
WHERE de.continent IS NOT NULL 
    AND TRIM(de.continent) != '';

-- Calculating the rolling percentage of people vaccinated vs population
SELECT 
	*, 
    (rolling_people_vaccinated/population)*100 AS rolling_pct_vaccinated_population
FROM pct_population_vaccinated

-- Create a view to store data for later visualizations
CREATE VIEW pct_population_vaccinated AS 
SELECT 
    de.continent, 
    de.location, 
    de.date, 
    de.population, 
    va.new_vaccinations,
    SUM(va.new_vaccinations) OVER (PARTITION BY de.location ORDER BY de.location, de.date) AS rolling_people_vaccinated
FROM portfolioproject.deaths AS de
JOIN portfolioproject.vacc AS va
    ON de.location = va.location AND de.date = va.date
WHERE de.continent IS NOT NULL 
    AND TRIM(de.continent) != ''

-- Checking view
SELECT *
FROM pct_population_vaccinated