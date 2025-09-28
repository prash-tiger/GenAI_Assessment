# DWBI SQL Generation with LLM (Groq API)

## Solution Overview
This solution uses the Groq API with the best available Llama model (`llama-3.1-70b-versatile`) to automatically generate SQL queries for business questions based on provided data warehouse schemas. The process is fully automated and does not require user configuration of the model or advanced settings.

### How It Works
- Loads schema definitions from `sales_dw.json` and `marketing_dw.json`.
- Loads business questions from `questions.csv`.
- Uses the Groq LLM to generate SQL queries, confidence scores, and reasoning for each question.
- Outputs results in CSV, JSON, and Markdown formats in the `output/` directory.

## Setup Instructions

1. **Clone or download this repository.**
2. **Install dependencies:**
   ```powershell
   pip install -r requirements.txt
   ```
3. **Set up your Groq API key:**
   - Create a `.env` file in the project root with the following content:
     ```env
     GROQ_API_KEY=your_groq_api_key_here
     ```
4. **Prepare your input files:**
   - Ensure `sales_dw.json`, `marketing_dw.json`, and `questions.csv` are present in the project directory.

## Usage
- Run the main script:
  ```powershell
  python app.py
  ```
- The script will prompt for your Groq API key (if not set in `.env`), process all questions, and save results in the `output/` folder.

## Output
- Results are saved as CSV, JSON, and Markdown reports in the `output/` directory.
- Each output includes: question ID, question, target source, generated SQL, assumptions, and confidence score.

## Notes
- The pipeline always uses the best Llama model available (no user selection required).
- You can add or modify questions in `questions.csv` and rerun the script.

## License
MIT
