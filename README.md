# automated-dcf-engine
This project bridges quantitative equity research and modern workflow automation. The objective was to eliminate the labor-intensive bottleneck of manual financial data entry and spreadsheet formatting by architecting a fully automated valuation pipeline. 

By utilizing agentic AI tools to facilitate the manual scripting, I directed the construction of a standalone application that extracts real-time data via `yfinance`, normalizes the capital structure, and dynamically generates a 5-year Discounted Cash Flow (DCF) model, DuPont Analysis, and Comparable Company matrix.

<img width="3838" height="2085" alt="DCF Engine Base" src="https://github.com/user-attachments/assets/fe01f594-8401-4b02-8788-775048f22c17" />

## Financial Methodology (The Analytical Logic)

The underlying calculation engine was designed around rigorous institutional valuation mechanics, prioritizing statistical accuracy over retail approximations:
- Auto-CAPM WACC generation utilizing live 10-Year Treasury yields and trailing beta.
- Geometric array progressions to model exponential revenue decay.
- Mid-year discounting conventions for continuous cash flow recognition.
- Toggleable terminal value logic supporting both Perpetuity Growth and Exit Multiples.
- Configurable tax shield normalization to account for expiring net operating losses.

## Automation Strategy & Core Features

Rather than manually coding the infrastructure, LLMs and agentic coding platforms were utilized to build the ETL pipelines, GUI, and export logic based on the required analytical parameters. 

The resulting application focuses on rapid scenario testing and functional Excel integration:
- **Scenario Comparison:** The pipeline features a custom memory-state architecture. An analyst can calculate a baseline model, lock it into memory, and immediately generate absolute and relative delta matrices against a newly calculated scenario. 
- **Dynamic Excel Injection:** Rather than simply printing flat numbers to a spreadsheet, the automated backend engine calculates spatial grid coordinates to inject live Excel formulas (such as NPV and complex conditionals) directly into the final export.

## Palantir (PLTR) Case Study

The repository includes a sample output file demonstrating a Base vs. Bull case scenario for Palantir (PLTR). 

- **Base Case:** Assumes margins remain near trailing twelve months, revenue growth exponentially decays to a 3% terminal rate, and terminal value is capped by Perpetuity Growth. 
- **Bull Case:** Tests a linear decay progression, scales margins to a 35% software target, and applies a 30x EV/EBITDA Exit Multiple. 

The automated software instantly quantifies the exact free cash flow divergence between these two theses across the timeline, allowing for immediate analytical review.
<img width="3838" height="2087" alt="DCF Engine Comparison Mode" src="https://github.com/user-attachments/assets/c3d543a5-99af-4e9f-adcc-c99832d03386" />

<img width="2693" height="1760" alt="Live Excel Scenario Comparison Output" src="https://github.com/user-attachments/assets/3e918f40-6342-4025-8d05-7a47f777586b" />

## Technical Toolkit

- **Quantitative Backend & ETL:** Python, Pandas, NumPy (Scripting facilitated via Agentic AI)
- **Data Pipeline:** `yfinance` 
- **GUI & Programmatic Export:** PyQt6, XlsxWriter
- **Binary Packaging:** PyInstaller

## Usage Instructions

To run the application natively on Windows without configuring an environment, download the `Institutional_DCF_Engine.exe` file from the Releases section of this repository and double-click to launch.

To run from source:
1. Clone the repository.
2. Create a Python 3.10 virtual environment.
3. Install the dependencies from `requirements.txt`.
4. Run `dcf.py`.
