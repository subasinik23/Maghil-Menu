import streamlit as st
import pandas as pd
import os
import uuid
import io # To handle file download in memory

# --- Helper Functions (Same as original script) ---
def convert_kids_friendly(value):
    """Converts a value to 'true' or 'false' for SQL boolean."""
    return 'true' if str(value).strip().lower() in ['yes', 'true', '1'] else 'false'

def extract_prep_time(value):
    """Extracts the second number from a range string like '5-10 mins' and formats as '0:MM'."""
    try:
        # Handle potential non-string values, None, NaN gracefully
        value_str = str(value) if pd.notna(value) else ""
        if '-' in value_str:
             # Split by '-' then by space, take the first part of the second split
            parts = value_str.split('-')
            if len(parts) > 1:
                 time_str = parts[1].strip().split()[0]
                 # Ensure it's a number before returning
                 if time_str.isdigit():
                      return f"0:{int(time_str):02d}" # Format with leading zero if needed
                 else:
                     return "0:00" # Return default if not a digit
        return "0:00" # Default if format doesn't match
    except Exception as e:
        st.warning(f"Error processing prep time '{value}': {e}")
        return "0:00" # Return default on error

# --- Main Logic Function ---
def generate_sql_queries(df, location_id):
    """Processes the DataFrame and generates all SQL queries."""
    all_sql_output = []

    # Step 3: Clean column names
    original_columns = list(df.columns)
    df.columns = df.columns.astype(str).str.strip().str.lower().str.replace(" ", "_")
    cleaned_columns = list(df.columns)

    all_sql_output.append(f"-- Original Columns: {original_columns}")
    all_sql_output.append(f"-- Cleaned Columns: {cleaned_columns}\n")

    # Step 4: Column mapping (using cleaned names)
    COLUMN_MAP = {
    "item_id": "item_id",
    "item_name": "item_name",
    "average_prep_time": "average_preparation_time",
    "is_kids_friendly": "is_kids_friendly",
    "special_instruction": "special_instruction",
    "allergic_info": "allergic_information",
    "spice_level": "spice_level",
    "ingredients": "ingredients",
    "pairing_recommendation": "pairing_recommendation"
}

    # Verify required columns exist after cleaning
    required_cols = list(COLUMN_MAP.values())
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns after cleaning: {missing_cols}. Check your Excel file headers.")

    # Step 6: Update queries for mh_items
    all_sql_output.append("-- Below are UPDATE queries for mh_items")
    all_sql_output.append("START TRANSACTION;") # Add transaction start
    update_queries = []
    for index, row in df.iterrows():
        try:
            item_id = str(row.get(COLUMN_MAP['item_id'], '')).strip()
            if not item_id:
                all_sql_output.append(f"-- Skipping row {index+2}: Missing item_id")
                continue # Skip rows with no item_id

            allergic_info = str(row.get(COLUMN_MAP['allergic_info'], '')).replace("'", "''").replace('"', '\\"') # Escape quotes
            is_kids_friendly = convert_kids_friendly(row.get(COLUMN_MAP['is_kids_friendly'], ''))
            prep_time = extract_prep_time(row.get(COLUMN_MAP['average_prep_time'], ''))
            special_instruction = str(row.get(COLUMN_MAP['special_instruction'], '')).replace("'", "''").replace('"', '\\"') # Escape quotes

            # Construct the JSON string manually
            attributes_json = (
                f'{{"allergicInfo": "{allergic_info}", '
                f'"kidsFriendly": {is_kids_friendly}, '
                f'"prepTimeInMins": "{prep_time}", '
                f'"specialInstructions": "{special_instruction}"}}'
            )
            # Escape single quotes in the final JSON string itself for the SQL query
            attributes_sql = attributes_json.replace("'", "''")


            query = (
                f"update mh_items set attributes = '{attributes_sql}' "
                f"where id = '{item_id}';"
            )
            update_queries.append(query)
        except Exception as e:
             all_sql_output.append(f"-- Error generating UPDATE for row {index+2} (item_id: {item_id}): {e}")
             continue # Continue with the next row

    all_sql_output.extend(update_queries)
    all_sql_output.append("COMMIT;") # Add transaction end
    all_sql_output.append("\n")

    # Step 8: Explode pairing recommendations
    all_sql_output.append("-- Below are INSERT queries for mh_item_recommendation")
    all_sql_output.append("START TRANSACTION;") # Add transaction start
    pairing_rows = []
    # Create a mapping from cleaned item names to cleaned item IDs
    item_name_to_id = df.set_index(COLUMN_MAP["item_name"])[COLUMN_MAP["item_id"]].to_dict()

    for index, row in df.iterrows():
        try:
            base_id = str(row.get(COLUMN_MAP["item_id"], '')).strip()
            rec_list_str = str(row.get(COLUMN_MAP["pairing_recommendation"], '')).strip()

            if not base_id:
                 all_sql_output.append(f"-- Skipping recommendations for row {index+2}: Missing base item_id")
                 continue

            if not rec_list_str:
                 # No recommendations for this item
                 continue

            rec_list = rec_list_str.split(',')

            for rec in rec_list:
                rec_name = rec.strip()
                if not rec_name:
                    continue
                recommended_id = item_name_to_id.get(rec_name, 'not found')

                if recommended_id != 'not found':
                    pairing_rows.append((base_id, recommended_id))
                else:
                    all_sql_output.append(f"-- Warning: Recommendation '{rec_name}' for item_id '{base_id}' not found in the Excel data.")
        except Exception as e:
             all_sql_output.append(f"-- Error processing recommendations for row {index+2} (item_id: {base_id}): {e}")
             continue

    # Step 9: Build one big INSERT query for recommendations
    if pairing_rows:
        # Delete existing recommendations for the items being processed first
        # This prevents duplicates if the script is run multiple times for the same items
        item_ids_with_recommendations = list(set([pair[0] for pair in pairing_rows]))
        if item_ids_with_recommendations:
             delete_query = f"DELETE FROM mh_item_recommendation WHERE item_id IN ({','.join([f"'{item}'" for item in item_ids_with_recommendations])});"
             all_sql_output.append(delete_query)

        insert_query = "INSERT INTO mh_item_recommendation (item_id, recommended_item_id) VALUES\n"
        values_part = ",\n".join([f"('{item}', '{rec}')" for item, rec in pairing_rows])
        full_insert = insert_query + values_part + ";"
        all_sql_output.append(full_insert)
    else:
        all_sql_output.append("-- No valid item recommendations found to insert.")

    all_sql_output.append("COMMIT;") # Add transaction end
    all_sql_output.append("\n")

    # ------------------- Task 3 & 4 -------------------
    all_sql_output.append("-- Below are queries for Spice Level Tags and their Media")
    all_sql_output.append("START TRANSACTION;") # Add transaction start

    # Step 10: Generate filter tags for unique spice levels
    # Use .get() and handle potential missing column gracefully
    spice_levels = df.get(COLUMN_MAP.get("spice_level"), pd.Series()).dropna().astype(str).str.strip().unique()
    spice_level_to_id = {level: str(uuid.uuid4()) for level in spice_levels if level} # Ensure level is not empty/whitespace

    insert_filter_tags = "-- No spice level filter tags to insert." # Default
    if spice_level_to_id:
        all_sql_output.append("-- INSERT for mh_filter_tag (spice levels)")
        insert_filter_tags_values = ",\n".join([
            f"('{guid}', '{location_id}', '{name.replace("'", "''")}', '1')" for name, guid in spice_level_to_id.items()
        ])
        insert_filter_tags = "INSERT INTO mh_filter_tag (id, location_id, name, is_food_prep) VALUES\n" + insert_filter_tags_values + ";"
        all_sql_output.append(insert_filter_tags)

    # Step 11: Create mh_item_filter_tag mappings
    item_filter_links = []
    if COLUMN_MAP.get("spice_level") in df.columns: # Check if the column exists
        for index, row in df.iterrows():
            try:
                item_id = str(row.get(COLUMN_MAP["item_id"], '')).strip()
                spice_level = str(row.get(COLUMN_MAP["spice_level"], '')).strip()
                filter_tag_id = spice_level_to_id.get(spice_level)

                if item_id and filter_tag_id:
                    item_filter_links.append((item_id, filter_tag_id))
                elif item_id and spice_level and not filter_tag_id:
                     # This case should not happen if spice_level_to_id was built correctly,
                     # but good for debugging if unique levels weren't captured.
                     pass
                # else: Missing item_id or no spice level for this item, skip

            except Exception as e:
                all_sql_output.append(f"-- Error processing item-filter link for row {index+2} (item_id: {item_id}): {e}")
                continue

    insert_item_filter_tags = "-- No item-filter tag links to insert." # Default
    if item_filter_links:
        # Delete existing links for the items being processed first
        item_ids_with_tags = list(set([link[0] for link in item_filter_links]))
        if item_ids_with_tags:
            # Optimization: Delete only for the tags we are inserting? Or just for the items?
            # Deleting just for the items is safer if tags might change.
            delete_item_tags_query = f"DELETE FROM mh_item_filter_tag WHERE item_id IN ({','.join([f"'{item}'" for item in item_ids_with_tags])});"
            all_sql_output.append(delete_item_tags_query)

        all_sql_output.append("-- INSERT for mh_item_filter_tag (spice mapping)")
        insert_item_filter_tags_values = ",\n".join([f"('{item}', '{tag}')" for item, tag in item_filter_links])
        insert_item_filter_tags = "INSERT INTO mh_item_filter_tag (item_id, filter_tag_id) VALUES\n" + insert_item_filter_tags_values + ";"
        all_sql_output.append(insert_item_filter_tags)
    else:
         all_sql_output.append("-- No item-filter tag links found to insert.")


    # Step 12: Generate mh_media entries for spice level filter tags
    media_queries = []
    if spice_level_to_id:
        all_sql_output.append("-- INSERT for mh_media (spice level filter images)")
        for spice_name, guid in spice_level_to_id.items():
            # Note: This assumes you have a default image file naming convention like GUID.png
            # You might need a more sophisticated way to map spice levels to actual images.
            # For this script, we'll use the GUID as the file name.
            query = (
                "INSERT INTO `mh_media` (`id`, `entity_type`, `entity_id`, `file_name`, `mime_type`) VALUES "
                f"('{guid}', 'FILTER', '{guid}', '{guid}.png', 'image/png');" # Use image/png or appropriate mime type
            )
            media_queries.append(query)
        all_sql_output.extend(media_queries)
    else:
         all_sql_output.append("-- No media entries to create for spice level tags.")

    all_sql_output.append("COMMIT;") # Add transaction end
    all_sql_output.append("\n")


    return "\n".join(all_sql_output)

# --- Streamlit App Layout ---
st.set_page_config(page_title="MenuHub Data Transformer", layout="wide")

st.title("🌶️ MenuHub Data Transformer")
st.write("Upload your Excel file and generate SQL queries for updating MenuHub data.")

# --- User Inputs ---
uploaded_file = st.file_uploader("Upload your Excel file (.xlsx or .xls)", type=["xlsx", "xls"])
location_id = st.text_input("Enter the Location ID", help="This is required for creating filter tags.")

# --- Processing Button ---
if st.button("Generate SQL Queries"):
    if uploaded_file is not None and location_id:
        with st.status("Processing Excel file and generating SQL...", expanded=True) as status:
            try:
                # Read the Excel file
                status.update(label="Reading Excel file...", state="running")
                df = pd.read_excel(uploaded_file)

                status.update(label="Generating SQL queries...", state="running")
                # Generate the SQL queries
                sql_output = generate_sql_queries(df, location_id)

                status.update(label="SQL queries generated successfully!", state="complete", expanded=False)

                st.success("SQL queries generated!")

                # --- Display and Download Output ---
                st.subheader("Generated SQL Queries")
                # Use an expander to show the code, as it can be long
                # with st.expander("View Generated SQL"):
                #      st.code(sql_output, language='sql')

                # Provide download button
                # Create a file-like object in memory
                sql_bytes = sql_output.encode('utf-8')
                sql_io = io.BytesIO(sql_bytes)

                st.download_button(
                    label="Download SQL File",
                    data=sql_io,
                    file_name="menu_hub_update_queries.sql",
                    mime="text/sql"
                )

            except ValueError as ve:
                st.error(f"Configuration Error: {ve}")
                status.update(label=f"Error: {ve}", state="error")
            except FileNotFoundError:
                 st.error("Internal Error: Could not find the Excel file. Please re-upload.")
                 status.update(label="Error: File not found.", state="error")
            except Exception as e:
                st.error(f"An unexpected error occurred: {e}")
                st.exception(e) # Show full traceback in console/logs
                status.update(label=f"Error: {e}", state="error")

    elif not uploaded_file:
        st.warning("Please upload an Excel file.")
    elif not location_id:
         st.warning("Please enter the Location ID.")

st.markdown("---")
st.write("Ensure your Excel file has the following columns (case-insensitive, spaces will be replaced with underscores):")
st.write("- Item ID")
st.write("- Item Name")
st.write("- Average Prep Time (e.g., '5-10 mins')")
st.write("- Is Kids Friendly (e.g., 'Yes'/'No' or 'True'/'False')")
st.write("- Special Instruction")
st.write("- Allergic Info")
st.write("- Spice Level")
st.write("- Pairing Recommendation (comma-separated list of Item Names)")
