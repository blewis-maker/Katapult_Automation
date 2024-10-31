import json
import http.client
import geopandas as gpd
from shapely.geometry import Point
import os
import requests
import time

API_KEY = "_EgKYvJtpOqWVmy4Hu31s4zrJL-qOqun6fHvm4tdsv9gATOYaXRyxskxtKZLe25VRb-Cn4k80fWVcah7v1DU99kNVht0_yXq28rohrVgTpIIApxLsgQq4q4rBxZb67sOVLfAmtWcvxa8IrR3nqfdzv6EUCBQxdbP4xTSXH_sNIHT9vxV805wtaqrRNxwVtVKnDP6LCAD6mr1__ufmrS5nx6Cdf2qNGBQzkoVf9IccL_uTytfaMqmWTP3YSIjGJu7IV24rNVMrJ0rBMbL8WmjdBEg88ceb1g2VR5dMgRMVL2PDE-MTSWLsiTTvu7x0iauJq3aDmPoQJACywgZ68tECA"

def getJobList():
    """Retrieve job list from KatapultPro API and include all jobs regardless of status."""
    conn = http.client.HTTPSConnection("katapultpro.com")
    headers = {}
    URL_PATH = '/api/v2/jobs'

    conn.request("GET", URL_PATH + "?api_key=" + API_KEY, headers=headers)
    res = conn.getresponse()
    data = res.read().decode("utf-8")

    try:
        jobs_dict = json.loads(data)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to decode JSON: {e}")

    if not isinstance(jobs_dict, dict):
        raise TypeError(f"Expected a dictionary but received {type(jobs_dict)}: {jobs_dict}")

    # Include all jobs regardless of their status
    all_jobs = [
        {'id': job_id, 'name': job_details.get('name'), 'status': job_details.get('status')}
        for job_id, job_details in jobs_dict.items()
    ]
    return all_jobs

def getJobData(job_id):
    """Retrieve job data for the given job ID."""
    conn = http.client.HTTPSConnection("katapultpro.com")
    headers = {}
    URL_PATH = f'/api/v2/jobs/{job_id}'

    for attempt in range(5):  # Retry up to 5 times
        conn.request("GET", URL_PATH + "?api_key=" + API_KEY, headers=headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")

        try:
            job_data = json.loads(data)
            if "error" in job_data:
                if job_data["error"] == "RATE LIMIT EXCEEDED":
                    print("Rate limit exceeded. Retrying after delay...")
                    time.sleep(5)  # Wait before retrying
                    continue  # Retry the request
            return job_data  # Ensure only job data is returned, not a tuple
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to decode JSON for job {job_id}: {e}")

    raise ValueError(f"Failed to retrieve job data for job ID {job_id} after multiple attempts.")

def extractPoles(job_data, job_name, job_id):
    """Extract points from job data, including 'Job_Status'."""
    print(f"Extracting poles from job: {job_name}, Job ID: {job_id}")
    if isinstance(job_data, tuple):
        job_data = job_data[1]  # Unpack if job_data is a tuple
    # Get Job_Status from job data
    job_status = job_data.get('metadata', {}).get('Job_Status', "Unknown")
    nodes = job_data.get("nodes", {})

    if not nodes:
        print("No nodes available or nodes is not a list.")
        return []

    pole_points = []
    for node_id, node_data in nodes.items():
        if node_data.get('attributes', {}).get('node_type', {}).get('button_added') == 'pole':
            latitude = node_data.get('latitude')
            longitude = node_data.get('longitude')
            int_note = node_data.get('attributes', {}).get('internal_note', {}).get('button_added')
            scid = node_data.get('attributes', {}).get('scid', {}).get('auto_button')

            # Extract the 'tagtext' from the 'pole_tag' dictionary
            pole_tag = None
            pole_tag_data = node_data.get('attributes', {}).get('pole_tag', {})
            if isinstance(pole_tag_data, dict):
                # Get the first dictionary value and access 'tagtext' if it exists
                first_tag = next(iter(pole_tag_data.values()), {})
                pole_tag = first_tag.get('tagtext')

            # Append attributes, Job_Status, and job_id for mapping Job_Name
            pole_points.append({
                "Longitude": longitude,
                "Latitude": latitude,
                "MRNote": int_note,
                "PoleTag": pole_tag,
                "SCID": scid,
                "Job_Status": job_status,
                "job_id": job_id  # Include job_id for mapping Job_Name later
            })
    return pole_points


def extractAnchors(job_data, job_name, job_id):
    """Extract anchor points from job data, focusing only on the Anchor_Spec attribute."""
    anchors = job_data.get("nodes", {})
    anchor_points = []

    for node_id, node_data in anchors.items():
        # Check if node_type is 'new anchor'
        if node_data.get("attributes", {}).get("node_type", {}).get("button_added") == "new anchor":
            latitude = node_data.get("latitude")
            longitude = node_data.get("longitude")

            # Extract anchor_spec
            anchor_spec_data = node_data.get("attributes", {}).get("anchor_spec", {})
            anchor_spec = anchor_spec_data.get("button_added", "Unknown")

            # Append anchor information with Anchor_Spec and Job_Name
            anchor_points.append({
                "Longitude": longitude,
                "Latitude": latitude,
                "Anchor_Spec": anchor_spec,  # Add Anchor_Spec to capture the anchor specification
                "job_id": job_id  # Include job_id for mapping Job_Name later
            })
    return anchor_points


def savePointsToShapefile(points, filename, job_dict):
    """Save points to a shapefile in the specified workspace using geopandas, with WGS 1984 CRS."""
    workspace_path = r"C:\Users\lewis\Documents\Deeply_Digital\Katapult_Automation\workspace"
    file_path = os.path.join(workspace_path, filename)

    print(f"Attempting to save shapefile to: {file_path}")

    # Create geometries from points with latitude and longitude
    geometries = [Point(point["Longitude"], point["Latitude"]) for point in points]

    # Map Job_Name and clean PoleTag
    for point in points:
        job_id = point.get("job_id")  # Retrieve job_id from point data
        point["Job_Name"] = job_dict.get(job_id, "Unknown")  # Set Job_Name or default to "Unknown"
        point["Job_Status"] = point.get("Job_Status", "Unknown")  # Include Job_Status for each point

        if isinstance(point.get("PoleTag"), dict) and "tagtext" in point["PoleTag"]:
            point["PoleTag"] = point["PoleTag"]["tagtext"]
        else:
            point["PoleTag"] = None

        # Debug output to verify Job_Name and Job_Status mappings
        print(f"Point Data - Job_Name: {point['Job_Name']}, Job_Status: {point['Job_Status']}")

    # Create GeoDataFrame with specified columns and geometries, and set CRS to WGS 1984 (EPSG:4326)
    gdf = gpd.GeoDataFrame(points, geometry=geometries, crs="EPSG:4326")

    try:
        gdf.to_file(file_path, driver="ESRI Shapefile")
        print(f"Shapefile successfully saved to: {file_path}")
    except Exception as e:
        print(f"Error saving shapefile: {e}")

def saveMasterShapefile(all_points, filename):
    """Save the combined list of points to a master shapefile, excluding the job_id field."""
    workspace_path = r"C:\Users\lewis\Documents\Deeply_Digital\Katapult_Automation\workspace"
    file_path = os.path.join(workspace_path, filename)

    # Remove the job_id field from each point
    for point in all_points:
        point.pop("job_id", None)  # Remove 'job_id' if it exists

    # Create geometries from points
    geometries = [Point(point["Longitude"], point["Latitude"]) for point in all_points]

    # Create GeoDataFrame and set CRS to WGS 1984 (EPSG:4326)
    gdf = gpd.GeoDataFrame(all_points, geometry=geometries, crs="EPSG:4326")

    try:
        gdf.to_file(file_path, driver="ESRI Shapefile")
        print(f"Master shapefile successfully saved to: {file_path}")
    except Exception as e:
        print(f"Error saving master shapefile: {e}")

def saveMasterAnchorShapefile(anchor_points, filename):
    """Save the combined list of anchor points to a master anchor shapefile, excluding unnecessary fields."""
    workspace_path = r"C:\Users\lewis\Documents\Deeply_Digital\Katapult_Automation\workspace"
    file_path = os.path.join(workspace_path, filename)

    # Remove the job_id field from each point
    for point in anchor_points:
        point.pop("job_id", None)  # Remove 'job_id' if it exists

        # Rename fields for Shapefile limitations
        if "Anchor_Spec" in point:
            point["Anch_Spec"] = point.pop("Anchor_Spec")  # Shorten to 10 characters

    # Create geometries from points
    geometries = [Point(point["Longitude"], point["Latitude"]) for point in anchor_points]

    # Create GeoDataFrame and set CRS to WGS 1984 (EPSG:4326)
    gdf = gpd.GeoDataFrame(anchor_points, geometry=geometries, crs="EPSG:4326")

    try:
        gdf.to_file(file_path, driver="ESRI Shapefile")
        print(f"Master anchor shapefile successfully saved to: {file_path}")
    except Exception as e:
        print(f"Error saving master anchor shapefile: {e}")


def main():
    TEST_FIRST_JOB_ONLY = False
    all_jobs = getJobList()
    all_pole_points = []  # List for storing pole points for the master shapefile
    all_anchor_points = []  # List for storing anchor points for the master shapefile

    # Create a job dictionary for mapping Job_Name by job_id
    job_dict = {job['id']: job['name'] for job in all_jobs}

    # Process each job
    for i, job in enumerate(all_jobs):
        job_id = job['id']
        job_name = job['name']

        print(f"Processing job: {job_name} (ID: {job_id})")

        # Fetch job data
        job_data = getJobData(job_id)
        if i == 0:
            print(json.dumps(job_data))  # Print JSON for first job

        # Extract pole and anchor points separately
        pole_points = extractPoles(job_data, job_name, job_id)
        anchor_points = extractAnchors(job_data, job_name, job_id)

        # Map Job_Name for each point before adding to master lists
        for point in pole_points:
            point["Job_Name"] = job_dict.get(job_id, "Unknown")  # Set Job_Name or default to "Unknown"
        all_pole_points.extend(pole_points)  # Add pole points to the master list

        for point in anchor_points:
            point["Job_Name"] = job_dict.get(job_id, "Unknown")  # Set Job_Name or default to "Unknown"
        all_anchor_points.extend(anchor_points)  # Add anchor points to the master list

        # Break after the first job if testing only the first job
        if TEST_FIRST_JOB_ONLY:
            break

    # Save the combined points to master shapefiles
    if all_pole_points:
        saveMasterShapefile(all_pole_points, 'master_poles.shp')
    else:
        print("No pole points found across jobs to save to master shapefile.")

    if all_anchor_points:
        saveMasterAnchorShapefile(all_anchor_points, 'master_anchors.shp')
    else:
        print("No anchor points found across jobs to save to master shapefile.")


if __name__ == '__main__':
    start_time = time.time()  # Record the start time
    main()
    end_time = time.time()  # Record the end time

    elapsed_time = end_time - start_time
    print(f"Total execution time: {elapsed_time:.2f} seconds")
