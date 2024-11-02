import json
import http.client
import geopandas as gpd
from shapely.geometry import Point, LineString
import os
import time
import socket
import re

API_KEY = 'rt2JR8Rds03Ry03hQTpD9j0N01gWEULJnuY3l1_GeXA8uqUVLtXsKHUQuW5ra0lt-FklrA40qq6_J04yY0nPjlfKG1uPerclUX2gf6axkIioJadYxzOG3cPZJLRcZ2_vHPdipZWvQdICAL2zRnqnOUCGjfq4Q8aMdmA7H6z7xK7W9MEKnIiEALokmtChLtr-s6hDFko17M7xihPpNlfGN7N8D___wn55epkLMtS2eFF3JPlj_SjpFIGXYK15PJFta-BmPqCFvEwXlZEYfEf8uYOpAvCEdBn3NOMoB-P28owOJ7ZeBQf5VMFi3J5_RV2fE_XDR2LTD469Qq0y3946LQ'

def getJobList():
    """Retrieve job list from KatapultPro API and include only the specified job."""
    URL_PATH = '/api/v2/jobs'
    headers = {}
    all_jobs = []

    specific_job_id = '-O-bszt-q6R0gSNTjCWw'  # ID for job FRM3_5 FRM_3-5

    for attempt in range(5):
        conn = None  # Initialize connection variable
        try:
            conn = http.client.HTTPSConnection("katapultpro.com", timeout=10)
            conn.request("GET", f"{URL_PATH}?api_key={API_KEY}", headers=headers)
            res = conn.getresponse()
            data = res.read().decode("utf-8")
            jobs_dict = json.loads(data)

            if not isinstance(jobs_dict, dict):
                raise TypeError(f"Expected a dictionary but received {type(jobs_dict)}: {jobs_dict}")

            # Filter to include only the specific job
            if specific_job_id in jobs_dict:
                job_details = jobs_dict[specific_job_id]
                all_jobs = [{'id': specific_job_id, 'name': job_details.get('name'), 'status': job_details.get('status')}]
            break  # Exit loop if successful

        except (socket.error, OSError) as e:
            print(f"Socket error: {e}. Retrying...")
            time.sleep(5)  # Wait before retrying
        except Exception as e:
            print(f"Failed to retrieve job list: {e}")
            break  # Exit on general exception
        finally:
            if conn:
                conn.close()  # Ensure the connection is closed

    return all_jobs


def getJobData(job_id):
    """Retrieve job data for the given job ID."""
    URL_PATH = f'/api/v2/jobs/{job_id}'
    headers = {}

    for attempt in range(5):
        conn = None  # Initialize connection variable
        try:
            conn = http.client.HTTPSConnection("katapultpro.com", timeout=10)
            conn.request("GET", f"{URL_PATH}?api_key={API_KEY}", headers=headers)
            res = conn.getresponse()
            data = res.read().decode("utf-8")
            job_data = json.loads(data)

            if "error" in job_data and job_data["error"] == "RATE LIMIT EXCEEDED":
                print("Rate limit exceeded. Retrying after delay...")
                time.sleep(5)
                continue
            return job_data  # Successfully retrieved data

        except json.JSONDecodeError:
            print(f"Failed to decode JSON for job {job_id}")
        except (socket.error, OSError) as e:
            print(f"Socket error while retrieving job data for {job_id}: {e}. Retrying...")
            time.sleep(5)  # Delay before retry
        except Exception as e:
            print(f"Error retrieving job data for {job_id}: {e}")
            break
        finally:
            if conn:
                conn.close()  # Ensure the connection is closed after each attempt
            time.sleep(0.5)  # Short delay between attempts

    print(f"Failed to retrieve job data for job ID {job_id} after multiple attempts.")
    return None


def extractPoles(job_data, job_name, job_id):
    """Extract poles from job data dynamically, focusing on identifying pole attributes and excluding reference nodes."""
    print(f"Extracting poles from job: {job_name}, Job ID: {job_id}")

    if isinstance(job_data, tuple):
        job_data = job_data[1]  # Unpack if job_data is a tuple

    # Get Job_Status from job data
    job_status = job_data.get('metadata', {}).get('job_status', "Unknown")
    nodes = job_data.get("nodes", {})

    if not nodes:
        print("No nodes available or nodes is not a dictionary.")
        return []

    pole_points = []
    for node_id, node_data in nodes.items():
        attributes = node_data.get('attributes', {})

        # Dynamic condition to detect if a node is a pole
        node_type = attributes.get('node_type', {}).get('button_added')
        has_pole_tag = 'pole_tag' in attributes
        is_pole = node_type == 'pole' or has_pole_tag or 'pole' in node_id.lower()

        # Retrieve SCID attribute and filter out reference nodes
        scid = attributes.get('scid', {}).get('auto_button', "")

        # Use regex to match SCIDs that resemble reference nodes (e.g., "107.A.A.A.A")
        is_reference_node = bool(re.match(r'^\d+(\.[A-Z])+$', scid))

        # Log node information to better understand why some nodes are being skipped
        if not is_pole or is_reference_node:
            print(
                f"Skipping node {node_id}: Node type is '{node_type}', has_pole_tag is '{has_pole_tag}', SCID is '{scid}' (Reference Node: {is_reference_node})")
            continue

        # If identified as a valid pole, extract necessary data
        latitude = node_data.get('latitude')
        longitude = node_data.get('longitude')

        if latitude is None or longitude is None:
            print(f"Skipping node {node_id}: Missing latitude or longitude")
            continue

        # Retrieve MR note, PoleTag, MR_Status, and Company attributes
        mr_note_data = attributes.get('mr_note', {})
        mr_note = next(iter(mr_note_data.values()), "")

        pole_tag = None
        pole_tag_data = attributes.get('pole_tag', {})
        if isinstance(pole_tag_data, dict):
            first_tag = next(iter(pole_tag_data.values()), {})
            pole_tag = first_tag.get('tagtext')

        # Retrieve MR_Status attribute
        mr_status_data = attributes.get('MR_status', {})
        mr_status = next(iter(mr_status_data.values()), "Unknown")

        # Retrieve Company attribute
        company_data = attributes.get('company', {})
        company = next(iter(company_data.values()), "Unknown")

        # Append attributes, Job_Status, MR_Status, Company, and job_id for mapping Job_Name
        pole_points.append({
            "Longitude": longitude,
            "Latitude": latitude,
            "MRNote": mr_note,
            "PoleTag": pole_tag,
            "SCID": scid,
            "Job_Status": job_status,
            "MR_Status": mr_status,
            "Company": company,
            "job_id": job_id
        })

    print(f"Total poles found: {len(pole_points)}")
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
        point["Job_Status"] = point.get("job_status", "Unknown")  # Include Job_Status for each point
        point["MR_Status"] = point.get("mr_status", "Unknown")  # Include Job_Status for each point
        point["Company"] = point.get("company", "Unknown")  # Include Job_Status for each point

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
    """Save the combined list of points to a master shapefile, including MR_Status, Company, and excluding job_id."""
    workspace_path = r"C:\Users\lewis\Documents\Deeply_Digital\Katapult_Automation\workspace"
    file_path = os.path.join(workspace_path, filename)

    # Define the expected columns with default values if missing
    for point in all_points:
        point.pop("job_id", None)
        point.setdefault("Job_Name", "Unknown")
        point.setdefault("Job_Status", "Unknown")
        point.setdefault("MR_Status", "Unknown")
        point.setdefault("Company", "Unknown")

    # Create geometries from points
    geometries = [Point(point["Longitude"], point["Latitude"]) for point in all_points]

    # Create GeoDataFrame with specific columns, ensuring MR_Status and Company are included
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


def extractConnections(job_data, job_name, job_id):
    """Extract line connections from job data using node references for start and end points, focusing on specific cable types."""
    connections = job_data.get("connections", {})
    nodes = job_data.get("nodes", {})
    cables = job_data.get("cables", {})

    print(f"Connections data type: {type(connections)}")

    if not isinstance(connections, dict) or not nodes:
        print(f"Unexpected structure in 'connections' or no nodes found.")
        return []

    line_connections = []

    for conn_id, connection in connections.items():
        # Extract connection type and filter references
        connection_type = connection.get("attributes", {}).get("connection_type", {}).get("value")
        if connection_type == "reference" or connection_type == "com reference":
            print(f"Skipping connection {conn_id} with type '{connection_type}'")
            continue

        # Fetch the node references for start and end
        start_node_id = connection.get("node_id_1")
        end_node_id = connection.get("node_id_2")

        start_node = nodes.get(start_node_id)
        end_node = nodes.get(end_node_id)

        if not start_node or not end_node:
            print(f"Missing node data for connection: start_node_id={start_node_id}, end_node_id={end_node_id}")
            continue

        start_coords = (start_node.get("longitude"), start_node.get("latitude"))
        end_coords = (end_node.get("longitude"), end_node.get("latitude"))

        if not all(start_coords) or not all(end_coords):
            print(f"Invalid coordinates: start={start_coords}, end={end_coords}")
            continue

        # Get SCID values for debugging
        start_scid = start_node.get('attributes', {}).get('scid', {}).get('auto_button', "Unknown")
        end_scid = end_node.get('attributes', {}).get('scid', {}).get('auto_button', "Unknown")

        # Log SCID information for debugging missing connections
        print(f"Processing connection {conn_id}: Start SCID={start_scid}, End SCID={end_scid}, Type={connection_type}")

        # Process cables to find specific wire_spec
        wire_spec = None
        cable_id = None
        for cable_id, cable_data in cables.items():
            # Match _trace to connection ID and fetch wire_spec
            if cable_data.get("_trace") == conn_id:
                wire_spec = cable_data.get("wire_spec", "No Spec Found")
                print(f"Found wire_spec '{wire_spec}' for cable ID {cable_id} in connection {conn_id}")
                break

        # Set connection type to "aerial cable" only if it's originally unknown
        if connection_type is None or connection_type.lower() == "unknown":
            connection_type = "aerial cable"

        line_connections.append({
            "StartX": start_coords[0],
            "StartY": start_coords[1],
            "EndX": end_coords[0],
            "EndY": end_coords[1],
            "ConnType": connection_type,
            "Wire_Spec": wire_spec,
            "JobName": job_name,
            "Start_SCID": start_scid,
            "End_SCID": end_scid
        })

    print(f"Total connections found: {len(line_connections)}")
    return line_connections


def saveLineShapefile(line_connections, filename, job_dict):
    """Save line connections to a shapefile with specified fields and geometry."""
    workspace_path = r"C:\Users\lewis\Documents\Deeply_Digital\Katapult_Automation\workspace"
    file_path = os.path.join(workspace_path, filename)

    print(f"Attempting to save line connections shapefile to: {file_path}")

    # Create line geometries
    geometries = [
        LineString([(line["StartX"], line["StartY"]), (line["EndX"], line["EndY"])])
        for line in line_connections
    ]

    # Prepare data for GeoDataFrame with specified fields
    line_data = [
        {
            "ConnType": line["ConnType"],
            "JobName": line["JobName"],
            "Wire_Spec": line["Wire_Spec"]
        }
        for line in line_connections
    ]

    # Create GeoDataFrame with specified columns and geometries, and set CRS to WGS 1984 (EPSG:4326)
    gdf = gpd.GeoDataFrame(line_data, geometry=geometries, crs="EPSG:4326")

    try:
        gdf.to_file(file_path, driver="ESRI Shapefile")
        print(f"Line shapefile successfully saved to: {file_path}")
    except Exception as e:
        print(f"Error saving line shapefile: {e}")


def main():
    TEST_FIRST_JOB_ONLY = True
    all_jobs = getJobList()
    all_pole_points = []  # List to store all pole points for the master shapefile
    all_anchor_points = []  # List to store all anchor points for the master shapefile
    all_line_connections = []  # List to store all line connections for the master shapefile

    # Create a job dictionary for mapping Job_Name by job_id
    job_dict = {job['id']: job['name'] for job in all_jobs}

    # Process each job
    for i, job in enumerate(all_jobs):
        job_id = job['id']
        job_name = job['name']

        print(f"Processing job: {job_name} (ID: {job_id})")

        # Fetch job data
        job_data = getJobData(job_id)

        # Print and save job data for reference
        if job_data:
            print(json.dumps(job_data))  # Pretty-print the JSON data

            # Save to a JSON file in the specified workspace path
            workspace_path = r"C:\Users\lewis\Documents\Deeply_Digital\Katapult_Automation\workspace"
            json_file_path = os.path.join(workspace_path, "job_data.json")
            with open(json_file_path, "w") as json_file:
                json.dump(job_data, json_file, indent=4)
            print(f"Job data saved to {json_file_path}")

        # If job_data retrieval was unsuccessful, skip to the next job
        if job_data is None:
            continue

        # Extract poles, anchors, and connections
        pole_points = extractPoles(job_data, job_name, job_id)
        anchor_points = extractAnchors(job_data, job_name, job_id)
        line_connections = extractConnections(job_data, job_name, job_id)

        # Map Job_Name for each point before adding to master lists
        for point in pole_points:
            point["Job_Name"] = job_dict.get(job_id, "Unknown")
        all_pole_points.extend(pole_points)  # Add pole points to the master list

        for point in anchor_points:
            point["Job_Name"] = job_dict.get(job_id, "Unknown")
        all_anchor_points.extend(anchor_points)  # Add anchor points to the master list

        # Add connections with job mapping
        for line in line_connections:
            line["Job_Name"] = job_dict.get(job_id, "Unknown")
        all_line_connections.extend(line_connections)  # Add line connections to the master list

        print(f"Total poles found: {len(pole_points)}")
        print(f"Total connections found: {len(line_connections)}")

        if TEST_FIRST_JOB_ONLY:
            break

        # Short delay between each job to reduce socket load
        time.sleep(1)

    # Save the combined points and lines to master shapefiles
    if all_pole_points:
        saveMasterShapefile(all_pole_points, 'master_poles.shp')
    else:
        print("No pole points found across jobs to save to master shapefile.")

    if all_anchor_points:
        saveMasterAnchorShapefile(all_anchor_points, 'master_anchors.shp')
    else:
        print("No anchor points found across jobs to save to master shapefile.")

    if all_line_connections:
        saveLineShapefile(all_line_connections, 'master_lines.shp', job_dict)
    else:
        print("No line connections found across jobs to save to master shapefile.")


if __name__ == '__main__':
    start_time = time.time()  # Record the start time
    main()
    end_time = time.time()  # Record the end time

    elapsed_time = end_time - start_time
    print(f"Total execution time: {elapsed_time:.2f} seconds")
