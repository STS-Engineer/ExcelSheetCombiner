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
CLEANUP_DIR = os.path.abspath("combined")

# === Helpers ===
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

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

# === Excel Processing Logic ===
def process_excel_files(uploaded_files, sheet_names_list, new_sheet_names_list=None, plant=None):
    combined_output = BytesIO()
    writer = pd.ExcelWriter(combined_output, engine='openpyxl')

    for idx, file in enumerate(uploaded_files):
        if file and allowed_file(file.filename):
            try:
                file_stream = BytesIO(file.read())
                xls = pd.ExcelFile(file_stream, engine='openpyxl')
                requested_sheets = sheet_names_list[idx]
                custom_names = new_sheet_names_list[idx] if new_sheet_names_list and idx < len(new_sheet_names_list) else []

                for i, sheet_name in enumerate(requested_sheets):
                    sheet_name_clean = sheet_name.strip()
                    if sheet_name_clean in xls.sheet_names:
                        try:
                            df_raw = pd.read_excel(xls, sheet_name=sheet_name_clean, engine='openpyxl', header=None)
                            df_trimmed = df_raw.iloc[2:].reset_index(drop=True)
                            df_trimmed.columns = df_trimmed.iloc[0]
                            df = df_trimmed.iloc[1:].reset_index(drop=True)

                            new_columns = {}
                            for col in df.columns:
                                col_lower = str(col).strip().lower()
                                if col_lower in ['day', 'inspect date']:
                                    new_columns[col] = 'date'
                            df.rename(columns=new_columns, inplace=True)

                            df.drop(columns=[col for col in df.columns if str(col).strip().lower() == 'type'], inplace=True)

                            # Use custom name if provided, else default to filename_sheetname
                            if i < len(custom_names):
                                new_sheet_name = custom_names[i].strip()[:31]
                            else:
                                new_sheet_name = f"{file.filename.rsplit('.', 1)[0]}_{sheet_name_clean}".replace(" ", "_")[:31]

                            df.to_excel(writer, sheet_name=new_sheet_name, index=False)

                        except Exception as e:
                            print(f"❌ Error processing sheet '{sheet_name_clean}' in file '{file.filename}': {e}")
                    else:
                        print(f"⚠️ Sheet '{sheet_name_clean}' not found in {file.filename}")
            except Exception as e:
                print(f"❌ Error processing file {file.filename}: {e}")

    writer.close()
    combined_output.seek(0)
    return combined_output

# === Reusable POST Logic ===
def handle_post_request(plant):
    uploaded_files = request.files.getlist('files')
    sheet_names_input = request.form.get('sheet_names')
    new_sheet_names_input = request.form.get('new_sheet_names')  # <-- new input

    if not sheet_names_input:
        return "Error: Sheet names input is required.", 400

    sheet_names_list = [group.strip().split(',') for group in sheet_names_input.strip().split(';')]
    new_sheet_names_list = [group.strip().split(',') for group in new_sheet_names_input.strip().split(';')] if new_sheet_names_input else None

    output = process_excel_files(uploaded_files, sheet_names_list, new_sheet_names_list, plant=plant)

    filename = f"{plant}_combined.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

# === Routes ===
@app.route('/kunshan', methods=['GET', 'POST'])
def kunshan():
    if request.method == 'POST':
        return handle_post_request('kunshan')
    return render_template('kunshan.html')

@app.route('/anhui', methods=['GET', 'POST'])
def anhui():
    if request.method == 'POST':
        return handle_post_request('anhui')
    return render_template('anhui.html')

# Optional default redirect to /kunshan
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        return handle_post_request('kunshan')
    return render_template('kunshan.html')
