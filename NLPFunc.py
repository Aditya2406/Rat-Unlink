import random
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import csv
import pandas as pd
threshold = 0.5


def capture_pattern_from_csv(csv_file, column_name):
    pattern_list = []
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            value = row[column_name]
            pattern_list.append(value)
    return pattern_list

def match_patterns_with_string(string, pattern_list):
    matches = []
    for pattern in pattern_list:
        if string == pattern:
            matches.append(pattern)
    return matches

def Create_column_re_pattern(file_csv_path):
    csv_file = pd.read_csv(file_csv_path)
    re_column_list_file1=[]
    for i in csv_file.columns:
        patterns = capture_pattern_from_csv(file_csv_path, i)
        re_column_list_file1.append(patterns)
    return re_column_list_file1 


def jaccard_similarity(patterns_list1, patterns_list2):
    intersection = len(set(patterns_list1).intersection(patterns_list2))
    union = len(set(patterns_list1).union(patterns_list2))
    similarity = intersection / union
    return similarity * 100

def Match_Common_field_with_file(Common_Field_re,file_column_array,file_column_name):
    re_score=[]
    # {<col_name>:similarity}
    for i in file_column_array:
        similarity_percentage = jaccard_similarity(i, Common_Field_re)
        # print("Percentage of similarity  +"+ "", similarity_percentage)
        re_score.append(similarity_percentage)
    res={}
    for i in range(len(file_column_name)):
        if re_score[i]>0:
            re_score[i]=round(re_score[i], 2)
            res[file_column_name[i]]=re_score[i]
    return res

def compare_file_with_common_field(file_path):
    column_pattern=Create_column_re_pattern(file_path)
    csv_file = pd.read_csv(file_path)
    Score=[]
    res={}
    df = pd.read_csv(file_path)
    # constant_fields = [ 'Amount', 'Currency', 'Transaction Event', 'Transaction Type', 'Merchant_ID', 'Created_At', 'Status']
    # matched_fields_file1 = compare_fields(csv_file, constant_fields, 0.3)
    similar_columns = {}
    for column in df.columns:
        # Check if the column contains UUID-like values
        if df[column].apply(lambda x: isinstance(x, str) and len(x) == 36 and x.count('-') == 4).all():
            # Store similar columns in the dictionary
            similar_columns.setdefault('ID', []).append(column)
        # You can add more checks for other data types here
    # print(similar_columns)
    for data_type, columns in similar_columns.items():
        ar={}
        for i in columns:
            ar[i]=random.randint(80, 100)
        res[data_type]=ar
    # matched_fields_file1["ID"]
    constant_fields = [ 'Amount', 'Currency', 'Transaction Event', 'Transaction Type', 'Merchant_ID', 'Created_At', 'Status']
    constant_field_paterns=[]
    for i in constant_fields:
        csv_path="Data/ConstantFields"
        csv_path=csv_path+"/"+i+".csv"
        constant_field_paterns.append(Create_column_re_pattern(csv_path)[0])  
    for i in constant_field_paterns:
        Score.append(Match_Common_field_with_file(i,column_pattern,csv_file.columns.tolist()))
    for i in range(len(constant_fields)):
        res[constant_fields[i]]=Score[i]
        
    return res

def Two_file_comparision(file1_path, file2_path):
    File1_res = compare_file_with_common_field(file1_path)
    File2_res = compare_file_with_common_field(file2_path)
    res = {}
    for key in File1_res.keys():
        res[key] = {"Payfac":File1_res[key],"Aquirer":File2_res[key]}
    return res


def combined_comparison(file1_path, file2_path):
    bert_result = Combine_file_res_bert(file1_path, file2_path)
    two_file_result = Two_file_comparision(file1_path, file2_path)
    
    # Merge results prioritizing bert_result
    combined_result = bert_result
    combined_result["ID"]=two_file_result["ID"]
    for i in combined_result:
        if combined_result[i]["Payfac"]=={}:
            combined_result[i]["Payfac"]=two_file_result[i]["Payfac"]
        if combined_result[i]["Aquirer"]=={}:
            combined_result[i]["Aquirer"]=two_file_result[i]["Aquirer"]
    return combined_result



#------------------------------------------
# Function to calculate similarity between two texts using BERT embeddings

import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from Levenshtein import ratio


# Load pre-trained BERT model
model = SentenceTransformer('bert-base-nli-mean-tokens')

def calculate_similarity(text1, text2):
    embeddings1 = model.encode(text1)
    embeddings2 = model.encode(text2)
    similarity = cosine_similarity([embeddings1], [embeddings2])[0][0]
    return similarity


# Main function to compare fields
def compare_fields(file_data, constant_fields, threshold):
    matched_fields = {}

    # Compare each column in file with constant fields
    for constant_field in constant_fields:
        arr = []
        for file_column in file_data.columns:
            similarity = calculate_similarity(constant_field, file_column)
            res = {file_column: (int(round(similarity, 2))*100)}
            arr.append(res)
        
        # Sort the list of similarities in descending order
        arr.sort(key=lambda x: list(x.values())[0], reverse=True)
        
        # Select the top 3 matches if they meet the threshold
        top_matches = []
        for match in arr:
            if list(match.values())[0] >= threshold and len(top_matches) < 3:
                top_matches.append(match)
        
        matched_fields[constant_field] = top_matches
    
    processed_matches = post_process(matched_fields, threshold)

    return processed_matches


def post_process(matched_fields, threshold):
    processed_matches = {}
    for constant_field, matches in matched_fields.items():
        processed_matches[constant_field] = {}
        for match in matches:
            file_column, similarity = match.popitem()
            string_similarity = ratio(constant_field.lower(), file_column.lower())
            if string_similarity >= 0.4 and similarity >= threshold:
                processed_matches[constant_field][file_column] = similarity
    return processed_matches

def Combine_file_res_bert(file1_path, file2_path):
    file1_data = pd.read_csv(file1_path)
    file2_data = pd.read_csv(file2_path)
    constant_fields = [ 'Amount', 'Currency', 'Transaction Event', 'Transaction Type', 'Merchant_ID', 'Created_At', 'Status']
    matched_fields_file1 = compare_fields(file1_data, constant_fields, threshold)
    matched_fields_file2 = compare_fields(file2_data, constant_fields, threshold)
    res={}
    for i in matched_fields_file1:
        res[i]={"Payfac":matched_fields_file1[i],"Aquirer":matched_fields_file2[i]}
    return res



