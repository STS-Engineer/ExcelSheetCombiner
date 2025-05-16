import os
import pandas as pd
from flask import Flask, render_template, request, send_file
from flask_apscheduler import APScheduler
from io import BytesIO

app = Flask(__name__)

# === Scheduler config ===
class Config:
    SCHEDULER_API_ENABLED = True

app.config.from_object(Config())
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

# === Constants ===
ALLOWED_EXTENSIONS = {'xlsx', 'xlsm', 'xltx', 'xltm'}
CLEANUP_DIR = os.path.abspath("combined")  # Change to your specific folder if needed

# === Helpers ===
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def normalize_sheet_name(filename, sheet_name):
    name = filename.rsplit('.', 1)[0]
    return f"{name}_{sheet_name}".replace(" ", "_")[:31]

def delete_excel_temp_files():
    for foldername, _, filenames in os.walk(CLEANUP_DIR):
        for filename in filenames:
            if filename.startswith('~$') and filename.endswith(('.xlsx', '.xlsm')):
                try:
                    os.remove(os.path.join(foldername, filename))
                    print(f"🧹 Deleted temp file: {filename}")
                except Exception as e:
                    print(f"❌ Could not delete {filename}: {e}")

# === Scheduled Job ===
@scheduler.task('interval', id='cleanup_job', minutes=10, misfire_grace_time=300)
def scheduled_cleanup():
    print("⏱️ Running scheduled Excel temp file cleanup...")
    delete_excel_temp_files()

# === Main Route ===
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        uploaded_files = request.files.getlist('files')
        sheet_names_input = request.form.get('sheet_names')
        sheet_names_list = [group.strip().split(',') for group in sheet_names_input.strip().split(';')]

        combined_output = BytesIO()
        writer = pd.ExcelWriter(combined_output, engine='openpyxl')

        for idx, file in enumerate(uploaded_files):
            if file and allowed_file(file.filename):
                try:
                    file_stream = BytesIO(file.read())
                    xls = pd.ExcelFile(file_stream, engine='openpyxl')

                    requested_sheets = sheet_names_list[idx]

                    for sheet_name in requested_sheets:
                        sheet_name_clean = sheet_name.strip()
                        if sheet_name_clean in xls.sheet_names:
                            df = pd.read_excel(xls, sheet_name=sheet_name_clean, engine='openpyxl')
                            new_sheet_name = normalize_sheet_name(file.filename, sheet_name_clean)
                            df.to_excel(writer, sheet_name=new_sheet_name, index=False)
                        else:
                            print(f"⚠️ Sheet '{sheet_name_clean}' not found in {file.filename}")
                except Exception as e:
                    print(f"❌ Error processing file {file.filename}: {e}")

        writer.close()
        combined_output.seek(0)

        return send_file(
            combined_output,
            as_attachment=True,
            download_name="combined_sheets.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    return render_template('index.html')
