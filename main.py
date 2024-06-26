from fastapi import FastAPI, File, UploadFile, HTTPException
from typing import Optional
import pandas as pd
import os

from NLPFunc import Two_file_comparision, combined_comparison
from mangum import Mangum

app = FastAPI()
handler=Mangum(app)

UPLOAD_FOLDER = "Sheet"

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def save_uploaded_file(file: UploadFile, file_name: str) -> Optional[str]:
    """
    Save the uploaded file to the specified folder.

    Parameters:
        file (UploadFile): The uploaded file object.
        file_name (str): The desired name of the file.

    Returns:
        str: The file path where the file is saved, or None if an error occurs.
    """
    try:
        file_path = os.path.join(UPLOAD_FOLDER, file_name)
        with open(file_path, "wb") as buffer:
            buffer.write(file.file.read())
        return file_path
    except Exception as e:
        print(f"Error saving file: {e}")
        return None

from io import BytesIO

@app.post("/Rat_Reccomendation/")
async def upload_files(file1: UploadFile = File(...), file2: UploadFile = File(...)):
    if file1.filename.endswith('.csv'):
        Aquirer_path = save_uploaded_file(file1, file1.filename)
    elif file1.filename.endswith('.xlsx'):
        file1_data = await file1.read()
        df = pd.read_excel(BytesIO(file1_data))
        Aquirer_path = os.path.join(UPLOAD_FOLDER, file1.filename[:-5] + ".csv")
        df.to_csv(Aquirer_path, index=False)
    else:
        raise HTTPException(status_code=400, detail="Unsupported file format for file1. Use .csv or .xlsx.")

    if file2.filename.endswith('.csv'):
        Payfac_path = save_uploaded_file(file2, file2.filename)
    elif file2.filename.endswith('.xlsx'):
        file2_data = await file2.read()
        df = pd.read_excel(BytesIO(file2_data))
        Payfac_path = os.path.join(UPLOAD_FOLDER, file2.filename[:-5] + ".csv")
        df.to_csv(Payfac_path, index=False)
    else:
        raise HTTPException(status_code=400, detail="Unsupported file format for file2. Use .csv or .xlsx.")
    
    comparison_result = combined_comparison(Aquirer_path, Payfac_path)
    print(comparison_result)
    # Delete the uploaded files after processing
    os.remove(Aquirer_path)
    os.remove(Payfac_path)
    
    return {"message": "Files Comparison Successful", "Comparison Result": comparison_result}

