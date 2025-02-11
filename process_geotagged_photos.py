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

def import_geotagged_photos_to_points(folder_path, output_folder_path):
    """
1) Gathers geotagged photos from folder_path.
2) Parses 'image_description' for the key 'folder'.
3) Groups photos by 'folder' value.
4) For each group (unique 'folder'):
- Creates a subdirectory: /root_output_folder/folder_value
- GPKG path: /root_output_folder/folder_value/folder_value.gpkg
* If GPKG doesn't exist, create columns for any new metadata keys.
* If GPKG exists, raise error if new metadata keys aren't in the file.
- Update/append points by filename.
- Save GPKG.
    """
    # 1. Collect photo data
    jpg_files = glob.glob(os.path.join(folder_path, '*.JPG')) \
        + glob.glob(os.path.join(folder_path, '*.jpg'))

    if not jpg_files:
        print("No JPG files found in", folder_path)
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

            # Parse the image_description into a dictionary
            image_description = getattr(img, 'image_description', None)
            desc_dict = parse_image_description(image_description)

            # We want a 'folder' key in the metadata. If missing, handle as needed.
            if 'folder' not in desc_dict:
                print(
                    f"Warning: 'folder' key not found in description for {photo_path}. "
                    "Skipping this photo (or raise an error instead)."
                )
                continue

            folder_value = desc_dict['folder']

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
                'folder': folder_value,  # keep folder if desired
                'parsed_dict': desc_dict  # store all parsed keys for later
            }
            all_records.append(record)
        else:
            print(f"Skipping {photo_path}: No valid GPS EXIF found.")

    if not all_records:
        print("No valid geotagged photos found with folder metadata.")
        return

    # 2. Group records by folder
    grouped_records = defaultdict(list)
    for record in all_records:
        grouped_records[record['folder']].append(record)

    # 3. For each folder group, create/update a GPKG
    for folder_value, records in grouped_records.items():
        # Create subdirectory: /root_output_folder/<folder_value>
        folder_output_dir = os.path.join(output_folder_path, folder_value)
        os.makedirs(folder_output_dir, exist_ok=True)

        # GPKG path: /root_output_folder/<folder_value>/<folder_value>.gpkg
        gpkg_path = os.path.join(folder_output_dir, f"{folder_value}.gpkg")

        # Gather distinct metadata keys
        all_meta_keys = set()
        for rec in records:
            all_meta_keys.update(rec['parsed_dict'].keys())

        # Try reading an existing GPKG
        if os.path.exists(gpkg_path):
            try:
                existing_gdf = gpd.read_file(gpkg_path)
                existing_columns = set(existing_gdf.columns)
            except Exception as e:
                print(f"Warning: Could not read existing file '{gpkg_path}': {e}")
                existing_gdf = None
                existing_columns = None
        else:
            existing_gdf = None
            existing_columns = None

        # If a GPKG exists, ensure new metadata keys are already in columns
        if existing_columns is not None:
            for k in all_meta_keys:
                if k not in existing_columns and k not in ('folder', 'parsed_dict'):
                    raise ValueError(
                        f"Image metadata key '{k}' does not match a point field in "
                        f"'{gpkg_path}'. Correct the key in the image or "
                        f"create a new field named '{k}' in this GPKG."
                    )

        # Build new_gdf
        new_gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")

        # If no existing data => create new columns for each metadata key
        if existing_gdf is None or existing_gdf.empty:
            # 1) Make sure these columns exist in new_gdf
            for meta_key in all_meta_keys:
                if meta_key not in ('folder', 'parsed_dict', 'image_description'):
                    # If it's not already present, create it
                    if meta_key not in new_gdf.columns:
                        new_gdf[meta_key] = None

            # 2) Populate those columns with the metadata values
            for i, row in new_gdf.iterrows():
                desc_dict = row['parsed_dict']
                for k, v in desc_dict.items():
                    if k in new_gdf.columns:
                        new_gdf.loc[i, k] = v
        else:
            # If GPKG exists, only update columns that are known
            for i, row in new_gdf.iterrows():
                desc_dict = row['parsed_dict']
                for k, v in desc_dict.items():
                    if k in existing_columns:
                        new_gdf.loc[i, k] = v

        # Remove 'parsed_dict' if not needed
        if 'parsed_dict' in new_gdf.columns:
            new_gdf.drop(columns=['parsed_dict'], inplace=True)

        # Merge or create fresh
        if existing_gdf is not None and not existing_gdf.empty:
            # Combine columns
            all_cols = list(set(existing_gdf.columns).union(set(new_gdf.columns)))
            existing_gdf = existing_gdf.reindex(columns=all_cols)
            new_gdf = new_gdf.reindex(columns=all_cols)

            # Update or append rows by matching 'filename'
            for i, new_row in new_gdf.iterrows():
                match = existing_gdf['filename'] == new_row['filename']
                if match.any():
                    idx = existing_gdf.index[match]
                    for col in all_cols:
                        if col != 'filename':
                            existing_gdf.loc[idx, col] = new_row[col]
                else:
                    existing_gdf = pd.concat([existing_gdf, new_row.to_frame().T],
                                             ignore_index=True)

            final_gdf = existing_gdf
        else:
            final_gdf = new_gdf

        # Save final GPKG
        final_gdf.set_crs(epsg=4326, inplace=True)
        final_gdf.to_file(gpkg_path, driver='GPKG')
        print(f"Saved {len(final_gdf)} records to '{gpkg_path}'.")

# Example usage:
if __name__ == "__main__":
    input_folder_path = '/Users/kanoalindiwe/Downloads/waiele/Final/GeotaggedPhotos'
    output_folder_path = "/Users/kanoalindiwe/Downloads/waiele/Final/"
    import_geotagged_photos_to_points(input_folder_path, output_folder_path)