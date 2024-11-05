import json
import http.client
import geopandas as gpd
from shapely.geometry import Point, LineString
import os
import time
import socket
import re

API_KEY = 'rt2JR8Rds03Ry03hQTpD9j0N01gWEULJnuY3l1_GeXA8uqUVLtXsKHUQuW5ra0lt-FklrA40qq6_J04yY0nPjlfKG1uPerclUX2gf6axkIioJadYxzOG3cPZJLRcZ2_vHPdipZWvQdICAL2zRnqnOUCGjfq4Q8aMdmA7H6z7xK7W9MEKnIiEALokmtChLtr-s6hDFko17M7xihPpNlfGN7N8D___wn55epkLMtS2eFF3JPlj_SjpFIGXYK15PJFta-BmPqCFvEwXlZEYfEf8uYOpAvCEdBn3NOMoB-P28owOJ7ZeBQf5VMFi3J5_RV2fE_XDR2LTD469Qq0y3946LQ'

# Function to get list of jobs from KatapultPro API
def getJobList():
    URL_PATH = '/api/v2/jobs'
    headers = {}
    all_jobs = []

    for attempt in range(5):  # Consider using exponential backoff to avoid overwhelming the server
        conn = None
        try:
            conn = http.client.HTTPSConnection("katapultpro.com", timeout=10)  # Timeout value could be made configurable
            conn.request("GET", f"{URL_PATH}?api_key={API_KEY}", headers=headers)
            res = conn.getresponse()
            data = res.read().decode("utf-8")
            jobs_dict = json.loads(data)

            if not isinstance(jobs_dict, dict):
                raise TypeError(f"Expected a dictionary but received {type(jobs_dict)}: {jobs_dict}")

            # Retrieve all jobs without filtering for status
            all_jobs = [
                {'id': job_id, 'name': job_details.get('name'), 'status': job_details.get('status')}
                for job_id, job_details in jobs_dict.items()
            ]
            break

        except (socket.error, OSError) as e:
            print(f"Socket error: {e}. Retrying...")
            time.sleep(5)
        except Exception as e:
            print(f"Failed to retrieve job list: {e}")
            break
        finally:
            if conn:
                conn.close()

    return all_jobs

# Function to get job data from KatapultPro API
def getJobData(job_id):
    URL_PATH = f'/api/v2/jobs/{job_id}'
    headers = {}

    for attempt in range(5):  # Consider using exponential backoff to avoid overwhelming the server
        conn = None
        try:
            conn = http.client.HTTPSConnection("katapultpro.com", timeout=10)  # Timeout value could be made configurable
            conn.request("GET", f"{URL_PATH}?api_key={API_KEY}", headers=headers)
            res = conn.getresponse()
            data = res.read().decode("utf-8")
            job_data = json.loads(data)

            if "error" in job_data and job_data["error"] == "RATE LIMIT EXCEEDED":
                print("Rate limit exceeded. Retrying after delay...")
                time.sleep(5)
                continue
            return job_data

        except json.JSONDecodeError:
            print(f"Failed to decode JSON for job {job_id}")
        except (socket.error, OSError) as e:
            print(f"Socket error while retrieving job data for {job_id}: {e}. Retrying...")
            time.sleep(5)
        except Exception as e:
            print(f"Error retrieving job data for {job_id}: {e}")
            break
        finally:
            if conn:
                conn.close()
            time.sleep(0.5)

    print(f"Failed to retrieve job data for job ID {job_id} after multiple attempts.")
    print(f"Job Data for {job_id}:\n{json.dumps(job_data, indent=2)}")
    return job_data
# Extract nodes (poles, anchors, etc.) from job data
# Extract nodes (poles, anchors, etc.) from job data
def extractNodes(job_data, job_name, job_id):
    nodes = job_data.get("nodes", {})
    if not nodes:
        print("No nodes found.")
        return []

    photo_data = job_data.get('photos', {})
    trace_data_all = job_data.get('traces', {}).get('trace_data', {})
    node_points = []

    for node_id, node_data in nodes.items():
        attributes = node_data.get('attributes', {})
        node_type = attributes.get('node_type', {}).get('-Imported')
        has_pole_tag = 'pole_tag' in attributes
        is_pole = (node_type == 'pole' or has_pole_tag or 'pole' in node_id.lower()) and node_type != 'reference'
        is_anchor = node_type == 'new anchor'

        if is_pole or is_anchor:
            latitude = node_data.get('latitude')
            longitude = node_data.get('longitude')

            if latitude is None or longitude is None:
                print(f"Skipping node {node_id}: Missing latitude or longitude")
                continue

            # Extract additional attributes to match the schema of "FRM_3-5_Nodes_pole"
            mr_status_data = attributes.get('MR_status', {})
            mr_status = next(iter(mr_status_data.values()), "Unknown")
            company = attributes.get('pole_tag', {}).get('-Imported', {}).get('company', "Unknown")
            fldcompl_value = attributes.get('field_completed', {}).get('value', "Unknown")
            fldcompl = 'yes' if fldcompl_value == 1 else 'no' if fldcompl_value == 2 else 'Unknown'
            pole_class = attributes.get('pole_class', {}).get('-Imported', "Unknown")
            pole_height = attributes.get('pole_height', {}).get('-Imported', "Unknown")
            pole_spec = attributes.get('pole_spec', {}).get('button_calced', "Unknown")
            tag = attributes.get('pole_tag', {}).get('-Imported', {}).get('tagtext', "Unknown")
            scid = attributes.get('scid', {}).get('auto_button', "Unknown")

            # Extract POA height using main photo wire data
            poa_height = ""

            # Locate the main photo
            photos = node_data.get('photos', {})
            main_photo_id = next(
                (photo_id for photo_id, photo_info in photos.items() if photo_info.get('association') == 'main'), None)

            if main_photo_id and main_photo_id in photo_data:
                photofirst_data = photo_data[main_photo_id].get('photofirst_data', {}).get('wire', {})
                for wire_id, wire_info in photofirst_data.items():
                    trace_id = wire_info.get('_trace')
                    trace_data = trace_data_all.get(trace_id, {})

                    # Check if the trace matches the desired conditions
                    if (trace_data.get('company') == 'Clearnetworx' and
                            trace_data.get('proposed', False) and
                            trace_data.get('_trace_type') == 'cable' and
                            trace_data.get('cable_type') == 'Fiber Optic Com'):

                        # Extract the measured height and convert to feet and inches
                        poa_height = wire_info.get('_measured_height')
                        if poa_height is not None:
                            feet = int(poa_height // 12)
                            inches = int(poa_height % 12)
                            poa_height = f"{feet}' {inches}\""
            # Extract POA height from "guying" if not found in wire
            if not poa_height and main_photo_id and main_photo_id in photo_data:
                guying_data = photo_data[main_photo_id].get('photofirst_data', {}).get('guying', {})
                for wire_id, wire_info in guying_data.items():
                    trace_id = wire_info.get('_trace')
                    trace_data = trace_data_all.get(trace_id, {})

                    # Check if the trace matches the desired conditions for down guy
                    if (trace_data.get('company') == 'Clearnetworx' and
                            trace_data.get('proposed', False) and
                            trace_data.get('_trace_type') == 'down_guy'):

                        # Extract the measured height and convert to feet and inches
                        poa_height = wire_info.get('_measured_height')
                        if poa_height is not None:
                            feet = int(poa_height // 12)
                            inches = int(poa_height % 12)
                            poa_height = f"{feet}' {inches}\""
                        break

            # Append the node data to the list
            node_points.append({
                "id": node_id,
                "lat": latitude,
                "lng": longitude,
                "jobname": job_name,
                "MR_statu": mr_status,
                "company": company,
                "fldcompl": fldcompl,
                "pole_class": pole_class,
                "tag": tag,
                "scid": scid,
                "POA_Height": poa_height
            })

    return node_points



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
                "longitude": longitude,
                "latitude": latitude,
                "anchor_spec": anchor_spec,
                "job_id": job_id
            })

    return anchor_points
# Extract connections (lines, cables, etc.) from job data

def extractConnections(job_data, job_name, job_id):
    connections = job_data.get("connections", {})
    nodes = job_data.get("nodes", {})
    photos = job_data.get("photos", {})

    if not isinstance(connections, dict) or not nodes:
        print("Unexpected structure in 'connections' or no nodes found.")
        return []

    line_connections = []

    for conn_id, connection in connections.items():
        attributes = connection.get("attributes", {})
        connection_type = attributes.get("connection_type", {}).get("value")

        if connection_type is None:
            connection_type = attributes.get("connection_type", {}).get("button_added")

        if connection_type is None:
            print(f"Connection ID {conn_id} has no 'connection_type' attribute. Full attributes: {attributes}")
            connection_type = "Unknown"
        elif not isinstance(connection_type, str):
            print(f"Connection ID {conn_id} has an unexpected type for 'connection_type': {type(connection_type)}. Full attributes: {attributes}")
            connection_type = "Unknown"
        else:
            print(f"Processing Connection ID {conn_id} with connection type: {connection_type}")

        # Skip non-aerial cable connections
        if connection_type != "aerial cable":
            continue

        # Extract start and end node IDs
        start_node_id = connection.get("node_id_1")
        end_node_id = connection.get("node_id_2")

        if not start_node_id or not end_node_id:
            print(f"Connection ID {conn_id} is missing start or end node ID.")
            continue

        start_node = nodes.get(start_node_id)
        end_node = nodes.get(end_node_id)

        if not start_node or not end_node:
            print(f"Missing node data for connection ID {conn_id}: start_node_id={start_node_id}, end_node_id={end_node_id}")
            continue

        # Extract start and end coordinates
        start_coords = (start_node.get("longitude"), start_node.get("latitude"))
        end_coords = (end_node.get("longitude"), end_node.get("latitude"))

        if not all(start_coords) or not all(end_coords):
            print(f"Invalid coordinates for connection ID {conn_id}")
            continue

        # Find the main photo associated with this connection in the midpoint_section
        main_photo_id = None
        sections = connection.get("sections", {}).get("midpoint_section", {})
        photos_dict = sections.get("photos", {})

        # Debugging: Print the photos dictionary for verification
        print(f"Photos dictionary for connection {conn_id}: {json.dumps(photos_dict, indent=2)}")

        # Loop through photos and find the one with association "main"
        for photo_id, photo_details in photos_dict.items():
            if photo_details.get("association") == "main":
                main_photo_id = photo_id
                print(f"Found main photo ID for connection {conn_id}: {main_photo_id}")
                break

        if not main_photo_id:
            print(f"No main photo found for connection ID {conn_id}")
            mid_ht = None
        else:
            # Use the main_photo_id to look up the photo in the "photos" dictionary
            main_photo_details = photos.get(main_photo_id, {})

            if not main_photo_details:
                print(f"Main photo ID {main_photo_id} not found in 'photos'. Available keys are: {list(photos.keys())}")
                mid_ht = None
            else:
                # Access the photofirst_data within the main_photo_details
                photofirst_entry = main_photo_details.get("photofirst_data", {})

                if not photofirst_entry:
                    print(f"Photofirst data not found for main photo ID {main_photo_id}.")
                    mid_ht = None
                else:
                    # Debugging: Print the photofirst entry found
                    print(f"Photofirst data for main photo ID {main_photo_id}: {json.dumps(photofirst_entry, indent=2)}")

                    wires = photofirst_entry.get("wire", {})
                    if not wires:
                        print(f"No wire data found in photofirst data for main photo ID {main_photo_id}.")
                        mid_ht = None
                    else:
                        # Debugging: Print the wire data in main photo
                        print(f"Wires data for main photo ID {main_photo_id}: {json.dumps(wires, indent=2)}")

                        # Step 4: Collect all "_trace" IDs
                        matching_trace_id = None

                        for wire_id, wire_info in wires.items():
                            trace_id = wire_info.get("_trace")
                            trace_data = job_data.get("traces", {}).get("trace_data", {}).get(trace_id, {})
                            company = trace_data.get("company")
                            proposed = trace_data.get("proposed")

                            # Debugging: Print trace information for each wire
                            print(f"Wire ID: {wire_id}, Trace ID: {trace_id}, Company: {company}, Proposed: {proposed}")

                            if company == "Clearnetworx" and proposed:
                                matching_trace_id = trace_id
                                break

                        if not matching_trace_id:
                            print(f"No matching trace found for connection ID {conn_id}.")
                            mid_ht = None
                        else:
                            # Step 5: Retrieve "_measured_height" for the wire matching the trace
                            for wire_id, wire_info in wires.items():
                                if wire_info.get("_trace") == matching_trace_id:
                                    mid_ht = wire_info.get("_measured_height")
                                    print(f"Found mid_ht for connection ID {conn_id}: {mid_ht}")
                                    if mid_ht is not None:
                                        feet = int(mid_ht // 12)
                                        inches = int(mid_ht % 12)
                                        mid_ht = f"{feet}' {inches}\""
                                    break

        # Append connection to list
        line_connections.append({
            "StartX": start_coords[0],
            "StartY": start_coords[1],
            "EndX": end_coords[0],
            "EndY": end_coords[1],
            "ConnType": connection_type,
            "JobName": job_name,
            "job_id": job_id,
            "mid_ht": mid_ht
        })

        print(f"Connection ID: {conn_id}, mid_ht: {mid_ht}")

    print(f"Total connections extracted: {len(line_connections)}")
    return line_connections


def savePointsToShapefile(points, filename):
    workspace_path = r"C:\Users\lewis\Documents\Deeply_Digital\Katapult_Automation\workspace"
    file_path = os.path.join(workspace_path, filename.replace('.shp', '.gpkg'))
    geometries = [Point(point["lng"], point["lat"]) for point in points]

    gdf = gpd.GeoDataFrame(points, geometry=geometries, crs="EPSG:4326")

    # Rename columns
    gdf.rename(columns={
        'company': 'utility',
        'tag': 'pole tag',
        'fldcompl': 'collected',
        'jobname': 'jobname',
        'MR_statu': 'mr_status',
        'pole_spec': 'pole_spec',
        'POA_Height': 'att_ht',
        'lat': 'latitude',
        'lng': 'longitude'
    }, inplace=True)

    # Remove unwanted columns, ignore if they don't exist
    gdf.drop(columns=['pole_class', 'pole_height', 'id'], errors='ignore', inplace=True)

    # Save to file
    try:
        gdf.to_file(file_path, driver="GPKG")  # Switched to GeoPackage for better flexibility
        print(f"GeoPackage successfully saved to: {file_path}")
    except Exception as e:
        print(f"Error saving GeoPackage: {e}")


# Function to save line connections to a GeoPackage
def saveAnchorsToGeoPackage(anchor_points, filename):
    workspace_path = r"C:\Users\lewis\Documents\Deeply_Digital\Katapult_Automation\workspace"
    file_path = os.path.join(workspace_path, filename.replace('.shp', '.gpkg'))
    geometries = [Point(anchor["longitude"], anchor["latitude"]) for anchor in anchor_points]

    gdf = gpd.GeoDataFrame(anchor_points, geometry=geometries, crs="EPSG:4326")

    # Rename columns
    gdf.rename(columns={
        'longitude': 'longitude',
        'latitude': 'latitude',
        'anchor_spec': 'anchor_spec'
    }, inplace=True)

    # Save to file
    try:
        gdf.to_file(file_path, layer='anchors', driver="GPKG")
        print(f"Anchors GeoPackage successfully saved to: {file_path}")
    except Exception as e:
        print(f"Error saving anchors GeoPackage: {e}")
def saveLineShapefile(line_connections, filename):
    workspace_path = r"C:\Users\lewis\Documents\Deeply_Digital\Katapult_Automation\workspace"
    file_path = os.path.join(workspace_path, filename.replace('.shp', '.gpkg'))
    geometries = [
        LineString([(line["StartX"], line["StartY"]), (line["EndX"], line["EndY"])]
    ) for line in line_connections]

    gdf = gpd.GeoDataFrame(line_connections, geometry=geometries, crs="EPSG:4326")
    gdf.drop(columns=['StartX', 'StartY', 'EndX', 'EndY', 'job_id'], errors='ignore', inplace=True)
    try:
        gdf.to_file(file_path, driver="GPKG")  # Switched to GeoPackage for better flexibility
        print(f"Line GeoPackage successfully saved to: {file_path}")
    except Exception as e:
        print(f"Error saving line GeoPackage: {e}")

# Function to save nodes to a GeoPackage
def saveMasterNodesToGeoPackage(all_nodes, filename):
    workspace_path = r"C:\Users\lewis\Documents\Deeply_Digital\Katapult_Automation\workspace"
    file_path = os.path.join(workspace_path, filename.replace('.shp', '.gpkg'))
    geometries = [Point(node["lng"], node["lat"]) for node in all_nodes]

    gdf = gpd.GeoDataFrame(all_nodes, geometry=geometries, crs="EPSG:4326")

    # Rename columns
    gdf.rename(columns={
        'company': 'utility',
        'tag': 'pole tag',
        'fldcompl': 'collected',
        'jobname': 'jobname',
        'job_status': 'job_status',
        'MR_statu': 'mr_status',
        'pole_spec': 'pole_spec',
        'POA_Height': 'att_ht',
        'lat': 'latitude',
        'lng': 'longitude'
    }, inplace=True)

    # Remove unwanted columns, ignore if they don't exist
    gdf.drop(columns=['pole_class', 'pole_height', 'id'], errors='ignore', inplace=True)

    # Save to file
    try:
        gdf.to_file(file_path, driver="GPKG")  # Switched to GeoPackage for better flexibility
        print(f"Master nodes GeoPackage successfully saved to: {file_path}")
    except Exception as e:
        print(f"Error saving master nodes GeoPackage: {e}")

# Function to save line connections to a GeoPackage
# Function to save line connections to a GeoPackage
def saveMasterConnectionsToGeoPackage(all_connections, filename):
    workspace_path = r"C:\Users\lewis\Documents\Deeply_Digital\Katapult_Automation\workspace"
    file_path = os.path.join(workspace_path, filename.replace('.shp', '.gpkg'))

    # Create geometries for each connection using StartX, StartY, EndX, and EndY
    geometries = [
        LineString([(line["StartX"], line["StartY"]), (line["EndX"], line["EndY"])])
        for line in all_connections
    ]

    # Create GeoDataFrame including mid_ht field
    gdf = gpd.GeoDataFrame(all_connections, geometry=geometries, crs="EPSG:4326")

    # Ensure mid_ht field is explicitly present in the DataFrame
    if 'mid_ht' not in gdf.columns:
        gdf['mid_ht'] = None  # Add the column if it doesn't exist

    # Drop unnecessary columns from connections
    gdf.drop(columns=['StartX', 'StartY', 'EndX', 'EndY', 'job_id'], errors='ignore', inplace=True)

    # Save to file
    try:
        gdf.to_file(file_path, driver="GPKG")  # Switched to GeoPackage for better flexibility
        print(f"Master connections GeoPackage successfully saved to: {file_path}")
    except Exception as e:
        print(f"Error saving master connections GeoPackage: {e}")

def saveMasterGeoPackage(all_nodes, all_connections, all_anchors, filename):
    workspace_path = r"C:\Users\lewis\Documents\Deeply_Digital\Katapult_Automation\workspace"
    file_path = os.path.join(workspace_path, filename.replace('.shp', '.gpkg'))

    # Save nodes as point layer
    if all_nodes:
        node_geometries = [Point(node["lng"], node["lat"]) for node in all_nodes]
        gdf_nodes = gpd.GeoDataFrame(all_nodes, geometry=node_geometries, crs="EPSG:4326")

        # Rename columns
        gdf_nodes.rename(columns={
            'company': 'utility',
            'tag': 'pole tag',
            'fldcompl': 'collected',
            'jobname': 'jobname',
            'job_status': 'job_status',
            'MR_statu': 'mr_status',
            'pole_spec': 'pole_spec',
            'POA_Height': 'att_ht',
            'lat': 'latitude',
            'lng': 'longitude'
        }, inplace=True)

        # Remove unwanted columns
        gdf_nodes.drop(columns=['pole_class', 'pole_height', 'id'], errors='ignore', inplace=True)

        if not gdf_nodes.empty:
            try:
                gdf_nodes.to_file(file_path, layer='poles', driver="GPKG", mode='w', OVERWRITE="YES")
                print(f"Nodes layer successfully saved to: {file_path}")
            except Exception as e:
                print(f"Error saving nodes layer to GeoPackage: {e}")

    # Save connections as line layer
    if all_connections:
        line_geometries = [
            LineString([(line["StartX"], line["StartY"]), (line["EndX"], line["EndY"])])
            for line in all_connections
        ]
        gdf_connections = gpd.GeoDataFrame(all_connections, geometry=line_geometries, crs="EPSG:4326")

        # Drop unnecessary columns from connections
        gdf_connections.drop(columns=['StartX', 'StartY', 'EndX', 'EndY', 'job_id'], errors='ignore', inplace=True)

        if not gdf_connections.empty:
            try:
                gdf_connections.to_file(file_path, layer='connections', driver="GPKG", mode='w', OVERWRITE="YES")
                print(f"Connections layer successfully saved to: {file_path}")
            except Exception as e:
                print(f"Error saving connections layer to GeoPackage: {e}")

    # Save anchors as point layer
    if all_anchors:
        anchor_geometries = [Point(anchor["longitude"], anchor["latitude"]) for anchor in all_anchors]
        gdf_anchors = gpd.GeoDataFrame(all_anchors, geometry=anchor_geometries, crs="EPSG:4326")

        # Rename columns
        gdf_anchors.rename(columns={
            'longitude': 'longitude',
            'latitude': 'latitude',
            'anchor_spec': 'anchor_spec'
        }, inplace=True)

        if not gdf_anchors.empty:
            try:
                gdf_anchors.to_file(file_path, layer='anchors', driver="GPKG", mode='w', OVERWRITE="YES")
                print(f"Anchors layer successfully saved to: {file_path}")
            except Exception as e:
                print(f"Error saving anchors layer to GeoPackage: {e}")
# Main function
# Main function to run the job for testing
def main():
    # Test a specific job ID
    test_job_id = "-O0l1U2kce21XnFGPvLy"
    all_nodes = []
    all_connections = []
    all_anchors = []

    job_name = "Test Job"
    print(f"Processing job: {job_name} (ID: {test_job_id})")

    job_data = getJobData(test_job_id)

    if job_data:
        nodes = extractNodes(job_data, job_name, test_job_id)
        connections = extractConnections(job_data, job_name, test_job_id)
        anchors = extractAnchors(job_data, job_name, test_job_id)

        if nodes:
            all_nodes.extend(nodes)
        if connections:
            all_connections.extend(connections)
        if anchors:
            all_anchors.extend(anchors)

    # Only save if data is present
    if all_nodes or all_connections or all_anchors:
        # Save all nodes, connections, and anchors to master GeoPackages
        saveMasterGeoPackage(all_nodes, all_connections, all_anchors, "Test_Master.gpkg")
    else:
        print("No data extracted for this job. Nothing to save.")

if __name__ == "__main__":
    start_time = time.time()  # Record the start time
    main()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total execution time: {elapsed_time:.2f} seconds")
