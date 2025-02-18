import os
import glob
import geopandas as gpd
import pandas as pd
from collections import defaultdict
from shapely.geometry import Point
from exif import Image as ExifImage  # pip install exif

def dms_to_dd(dms_tuple, ref):
    """
    Convert (degrees, minutes, seconds) + reference (e.g., 'N'/'S' or 'E'/'W')
    into decimal degrees.
    """
    degrees, minutes, seconds = dms_tuple
    dd = degrees + minutes / 60.0 + seconds / 3600.0
    if ref in ['S', 'W']:
        dd = -dd
    return dd

def parse_image_description(description):
    """
    Given a string like "key1-value1;key2-value2", parse and return a dict:
    { "key1": "value1", "key2": "value2" }
    Ignores any empty or badly formed segments.
    """
    meta_dict = {}
    if not description:
        return meta_dict

    pairs = description.split(';')
    for pair in pairs:
        pair = pair.strip()
        if not pair:
            continue
        kv = pair.split('-', 1)  # split on the first dash only
        if len(kv) == 2:
            key, value = kv
            key = key.strip()
            value = value.strip()
            meta_dict[key] = value
    return meta_dict

def import_geotagged_photos_to_points(
        input_folder_path,
        output_folder_path,
        folder_key_word,
        folder_valid_values=None  # New optional parameter.
):
    """
1) Gathers geotagged photos from input_folder_path.
2) Uses 'folder_key_word' (case-insensitive) to find the grouping key in
image_description, forcing the final column name to lowercase.
3) Groups photos by that key's value.
4) For each group:
- Creates a subdirectory: /root_output_folder/<group_value>
- GPKG path: /root_output_folder/<group_value>/<group_value>.gpkg
* If GPKG doesn't exist, create columns for any new metadata keys.
* If GPKG exists, rename 'F' => 'f' if needed, raise an error if new keys aren't in the file.
- Update/append points by matching 'filename'.
- Remove old records whose filename is not in the new set.
- Save the GPKG, avoiding the duplicate column name error.

:param folder_valid_values: Optional list of allowed values for the folder grouping.
If provided, only images with a group value in this list are processed.
"""

    # Always use a lowercase version for the final column name
    final_key = folder_key_word.lower()

    # 1. Gather all JPG files
    jpg_files = glob.glob(os.path.join(input_folder_path, '*.JPG')) \
        + glob.glob(os.path.join(input_folder_path, '*.jpg'))

    if not jpg_files:
        print(f"No JPG files found in {input_folder_path}")
        return

    all_records = []

    # Parse each photo
    for photo_path in jpg_files:
        with open(photo_path, 'rb') as f:
            img = ExifImage(f)

        if img.has_exif and hasattr(img, 'gps_latitude') and hasattr(img, 'gps_longitude'):
            lat = dms_to_dd(img.gps_latitude, getattr(img, 'gps_latitude_ref', 'N'))
            lon = dms_to_dd(img.gps_longitude, getattr(img, 'gps_longitude_ref', 'E'))

            alt = getattr(img, 'gps_altitude', None)
            if isinstance(alt, tuple) and len(alt) == 2:  # ratio
                alt = alt[0] / alt[1]

            # Parse the image_description and normalize keys to lowercase
            image_description = getattr(img, 'image_description', None)
            desc_dict = {k.lower(): v for k, v in parse_image_description(image_description).items()}

            # Check if final_key exists
            if final_key not in desc_dict:
                print(
                    f"Warning: '{folder_key_word}' key (any case) not found "
                    f"in description for {photo_path}. Skipping."
                )
                continue

            group_value = desc_dict[final_key]

            # --- New Check ---
            # If folder_valid_values is provided, check if the group_value is allowed.
            if folder_valid_values is not None and group_value not in folder_valid_values:
                raise ValueError(
                    f"Invalid folder value '{group_value}' for photo '{photo_path}'. "
                    f"Allowed values are: {folder_valid_values}"
                )

            record = {
                'filename': os.path.basename(photo_path),
                'latitude': lat,
                'longitude': lon,
                'altitude': alt,
                'image_description': image_description,
                'create_date': getattr(img, 'datetime_original', None),
                'orientation': getattr(img, 'orientation', None),
                # 3D geometry if altitude present
                'geometry': Point(lon, lat, alt) if alt is not None else Point(lon, lat),
                # Force the grouping column to final_key (lowercase)
                final_key: group_value,
                'parsed_dict': desc_dict,
            }
            all_records.append(record)
        else:
            print(f"Skipping {photo_path}: No valid GPS EXIF found.")

    if not all_records:
        print(f"No valid geotagged photos found with '{folder_key_word}' metadata.")
        return

    # 2. Group records by the final_key
    grouped_records = defaultdict(list)
    for record in all_records:
        grouped_records[record[final_key]].append(record)

    # 3. For each group, create/update a GPKG
    for group_value, records in grouped_records.items():
        # Subdirectory
        folder_output_dir = os.path.join(output_folder_path, group_value)
        os.makedirs(folder_output_dir, exist_ok=True)

        # Output GPKG path
        gpkg_path = os.path.join(folder_output_dir, f"{group_value}.gpkg")

        # Gather distinct metadata keys from these records
        all_meta_keys = set()
        for rec in records:
            all_meta_keys.update(rec['parsed_dict'].keys())

        # Attempt to read existing GPKG
        if os.path.exists(gpkg_path):
            try:
                existing_gdf = gpd.read_file(gpkg_path)
                existing_columns = list(existing_gdf.columns)
            except Exception as e:
                print(f"Warning: Could not read existing file '{gpkg_path}': {e}")
                existing_gdf = None
                existing_columns = []
        else:
            existing_gdf = None
            existing_columns = []

        # --- NEW STEP: If the existing GPKG has a column "F" that conflicts with "f", rename it. ---
        # Because GeoPackage is case-insensitive, "F" and "f" collide. We'll unify them to final_key.
        renamed_cols = {}
        # We'll check for any columns that match final_key case-insensitively
        for col in existing_columns:
            if col.lower() == final_key and col != final_key:
                # e.g., col = "F", final_key = "f"
                # If there's already a "f" column, unify them
                if final_key in existing_columns:
                    # unify data if needed
                    existing_gdf[final_key] = existing_gdf[final_key].fillna(existing_gdf[col])
                    # drop the old column
                    existing_gdf.drop(columns=[col], inplace=True)
                else:
                    renamed_cols[col] = final_key

        if renamed_cols:
            existing_gdf.rename(columns=renamed_cols, inplace=True)
            # update columns list
            existing_columns = list(existing_gdf.columns)

        # If GPKG still exists, ensure new metadata keys are already in columns
        if existing_gdf is not None and not existing_gdf.empty:
            for k in all_meta_keys:
                if k not in (final_key, 'parsed_dict', 'image_description'):
                    # Because it's case-insensitive, compare lower
                    col_lowers = [c.lower() for c in existing_columns]
                    if k.lower() not in col_lowers:
                        raise ValueError(
                            f"Image metadata key '{k}' does not match a point field in "
                            f"'{gpkg_path}'. Correct the key in the image or "
                            f"create a new field named '{k}' in this GPKG."
                        )

        # Build new_gdf from the records
        new_gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")

        # If no existing data => create new columns
        if existing_gdf is None or existing_gdf.empty:
            for meta_key in all_meta_keys:
                if meta_key not in (final_key, 'parsed_dict', 'image_description'):
                    if meta_key not in new_gdf.columns:
                        new_gdf[meta_key] = None

            # Populate
            for i, row in new_gdf.iterrows():
                desc_dict = row['parsed_dict']
                for k, v in desc_dict.items():
                    if k in new_gdf.columns:
                        new_gdf.loc[i, k] = v
        else:
            # If GPKG exists, only update columns that are known
            existing_lower_map = {c.lower(): c for c in existing_columns}
            for i, row in new_gdf.iterrows():
                desc_dict = row['parsed_dict']
                for k, v in desc_dict.items():
                    kl = k.lower()
                    if kl in existing_lower_map:
                        actual_col = existing_lower_map[kl]
                        new_gdf.loc[i, actual_col] = v

        # Remove 'parsed_dict' column if not needed
        if 'parsed_dict' in new_gdf.columns:
            new_gdf.drop(columns=['parsed_dict'], inplace=True)

        # Merge or create fresh
        if existing_gdf is not None and not existing_gdf.empty:
            all_cols = list(set(existing_gdf.columns) | set(new_gdf.columns))
            existing_gdf = existing_gdf.reindex(columns=all_cols)
            new_gdf = new_gdf.reindex(columns=all_cols)

            # Update or append by matching 'filename'
            for i, new_row in new_gdf.iterrows():
                match = existing_gdf['filename'] == new_row['filename']
                if match.any():
                    idx = existing_gdf.index[match]
                    for col in all_cols:
                        if col != 'filename':
                            existing_gdf.loc[idx, col] = new_row[col]
                else:
                    existing_gdf = pd.concat(
                        [existing_gdf, new_row.to_frame().T], ignore_index=True
                    )

            # Remove old records not present in new_gdf
            new_filenames = set(new_gdf['filename'])
            existing_gdf = existing_gdf[existing_gdf['filename'].isin(new_filenames)]

            final_gdf = existing_gdf
        else:
            final_gdf = new_gdf

        # Save final GPKG
        final_gdf.set_crs(epsg=4326, inplace=True)
        final_gdf.to_file(gpkg_path, driver='GPKG')
        print(f"Saved {len(final_gdf)} records to '{gpkg_path}'.")


# Example usage:
if __name__ == "__main__":
    input_folder_path = '/Users/kanoalindiwe/Downloads/waiele/waiele_project/geotagged_photos'
    output_folder_path = "/Users/kanoalindiwe/Downloads/waiele/waiele_project"
    folder_key_word = 'F' # or lower/upper case
    # folder_valid_values = "None" for no filter or ['plansi', 'otherallowedvalue'] for filtering
    folder_valid_values = ['plantss', 'plantsi', 'fauna', 'arch', 'debris']

    import_geotagged_photos_to_points(
        input_folder_path,
        output_folder_path,
        folder_key_word,
        folder_valid_values
    )