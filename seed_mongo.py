from pymongo import MongoClient, ASCENDING
from datetime import datetime

def dt(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

client = MongoClient("mongodb://localhost:27017")
db = client["assetsdb"]

# ── Assets ────────────────────────────────────────────────────────────────────
assets = [
    {
        "assetId": "ASSET-001", "name": "Main Feed Pump A", "type": "Pump",
        "category": "Rotating Equipment", "manufacturer": "Flowserve", "model": "PVXM-220",
        "serialNumber": "FLW-2021-00441", "status": "operational",
        "description": "Primary centrifugal feed pump for Unit 1 crude distillation",
        "locationId": "LOC-001", "installDate": dt("2021-03-15T00:00:00Z"),
        "lastMaintenanceDate": dt("2025-09-10T00:00:00Z"),
        "tags": ["critical", "unit1", "rotating"],
        "specs": {"powerKw": 75, "flowRateLpm": 1200, "maxPressureBar": 16},
        "createdAt": dt("2021-03-15T10:00:00Z"), "updatedAt": dt("2025-09-10T08:30:00Z")
    },
    {
        "assetId": "ASSET-002", "name": "Overhead Compressor B", "type": "Compressor",
        "category": "Rotating Equipment", "manufacturer": "Atlas Copco", "model": "GA-315",
        "serialNumber": "AC-2020-88712", "status": "operational",
        "description": "Reciprocating compressor for overhead gas recovery",
        "locationId": "LOC-001", "installDate": dt("2020-07-20T00:00:00Z"),
        "lastMaintenanceDate": dt("2025-11-01T00:00:00Z"),
        "tags": ["critical", "unit2", "rotating"],
        "specs": {"powerKw": 315, "capacityNm3h": 4500, "maxPressureBar": 25},
        "createdAt": dt("2020-07-20T10:00:00Z"), "updatedAt": dt("2025-11-01T14:00:00Z")
    },
    {
        "assetId": "ASSET-003", "name": "Shell & Tube Heat Exchanger E-101", "type": "Heat Exchanger",
        "category": "Static Equipment", "manufacturer": "Alfa Laval", "model": "STE-HXR-101",
        "serialNumber": "AL-2019-33210", "status": "maintenance",
        "description": "Pre-heat exchanger for crude feed - currently offline for tube bundle replacement",
        "locationId": "LOC-001", "installDate": dt("2019-11-05T00:00:00Z"),
        "lastMaintenanceDate": dt("2026-03-01T00:00:00Z"),
        "tags": ["unit1", "static", "heat-transfer"],
        "specs": {"surfaceAreaM2": 340, "shellSideBarG": 12, "tubeSideBarG": 8},
        "createdAt": dt("2019-11-05T10:00:00Z"), "updatedAt": dt("2026-03-01T09:00:00Z")
    },
    {
        "assetId": "ASSET-004", "name": "Control Valve FV-204", "type": "Valve",
        "category": "Instrumentation", "manufacturer": "Emerson", "model": "Fisher D4",
        "serialNumber": "EMR-2022-11045", "status": "operational",
        "description": "Flow control valve on distillate draw-off line",
        "locationId": "LOC-001", "installDate": dt("2022-01-10T00:00:00Z"),
        "lastMaintenanceDate": dt("2025-08-20T00:00:00Z"),
        "tags": ["instrumentation", "unit1", "control"],
        "specs": {"sizInch": 4, "cvMax": 120, "failPosition": "fail-close"},
        "createdAt": dt("2022-01-10T10:00:00Z"), "updatedAt": dt("2025-08-20T11:00:00Z")
    },
    {
        "assetId": "ASSET-005", "name": "Pressure Transmitter PT-101", "type": "Sensor",
        "category": "Instrumentation", "manufacturer": "Yokogawa", "model": "EJA110E",
        "serialNumber": "YOK-2023-50012", "status": "operational",
        "description": "High-accuracy pressure transmitter on reactor inlet",
        "locationId": "LOC-001", "installDate": dt("2023-02-14T00:00:00Z"),
        "lastMaintenanceDate": dt("2025-12-05T00:00:00Z"),
        "tags": ["sensor", "unit1", "pressure"],
        "specs": {"rangeBar": "0-40", "accuracyPct": 0.055, "outputSignal": "4-20mA HART"},
        "createdAt": dt("2023-02-14T10:00:00Z"), "updatedAt": dt("2025-12-05T07:45:00Z")
    },
    {
        "assetId": "ASSET-006", "name": "Forklift FL-06", "type": "Vehicle",
        "category": "Material Handling", "manufacturer": "Toyota", "model": "8FBE20",
        "serialNumber": "TOY-2021-WH0892", "status": "operational",
        "description": "Electric counterbalance forklift assigned to Bay 3",
        "locationId": "LOC-002", "installDate": dt("2021-06-01T00:00:00Z"),
        "lastMaintenanceDate": dt("2026-01-15T00:00:00Z"),
        "tags": ["vehicle", "warehouse", "bay3"],
        "specs": {"capacityKg": 2000, "liftHeightMm": 5000, "batteryVoltage": 48},
        "createdAt": dt("2021-06-01T10:00:00Z"), "updatedAt": dt("2026-01-15T08:00:00Z")
    },
    {
        "assetId": "ASSET-007", "name": "Conveyor Belt CB-07", "type": "Conveyor",
        "category": "Material Handling", "manufacturer": "Hytrol", "model": "EZLogic ZPA",
        "serialNumber": "HYT-2022-44500", "status": "operational",
        "description": "Zero-pressure accumulation conveyor for outbound sorting area",
        "locationId": "LOC-002", "installDate": dt("2022-09-15T00:00:00Z"),
        "lastMaintenanceDate": dt("2025-10-20T00:00:00Z"),
        "tags": ["conveyor", "warehouse", "sorting"],
        "specs": {"lengthM": 45, "widthMm": 600, "speedMpm": 30},
        "createdAt": dt("2022-09-15T10:00:00Z"), "updatedAt": dt("2025-10-20T13:00:00Z")
    },
    {
        "assetId": "ASSET-008", "name": "HVAC Unit AHU-08", "type": "HVAC",
        "category": "Facilities", "manufacturer": "Carrier", "model": "WeatherMaster 50XC",
        "serialNumber": "CAR-2020-CLI1023", "status": "operational",
        "description": "Air handling unit for main office floor 3",
        "locationId": "LOC-003", "installDate": dt("2020-04-10T00:00:00Z"),
        "lastMaintenanceDate": dt("2025-07-12T00:00:00Z"),
        "tags": ["facilities", "hvac", "floor3"],
        "specs": {"coolingTonnes": 20, "airflowCfm": 8000, "refrigerant": "R-410A"},
        "createdAt": dt("2020-04-10T10:00:00Z"), "updatedAt": dt("2025-07-12T10:30:00Z")
    },
    {
        "assetId": "ASSET-009", "name": "UPS System UPS-09", "type": "Power",
        "category": "Electrical", "manufacturer": "Eaton", "model": "9395P-1100",
        "serialNumber": "ETN-2021-UPS7741", "status": "operational",
        "description": "1.1 MVA UPS protecting data centre and critical servers",
        "locationId": "LOC-003", "installDate": dt("2021-11-20T00:00:00Z"),
        "lastMaintenanceDate": dt("2026-02-14T00:00:00Z"),
        "tags": ["electrical", "critical", "datacenter"],
        "specs": {"powerKva": 1100, "runtimeMinutes": 15, "topology": "double-conversion"},
        "createdAt": dt("2021-11-20T10:00:00Z"), "updatedAt": dt("2026-02-14T09:00:00Z")
    },
    {
        "assetId": "ASSET-010", "name": "Spectrometer SP-10", "type": "Analyser",
        "category": "Laboratory Equipment", "manufacturer": "Thermo Fisher", "model": "Nicolet iS50",
        "serialNumber": "TF-2023-FTIR0044", "status": "operational",
        "description": "FT-IR spectrometer for polymer and material characterization",
        "locationId": "LOC-004", "installDate": dt("2023-06-01T00:00:00Z"),
        "lastMaintenanceDate": dt("2025-11-30T00:00:00Z"),
        "tags": ["lab", "analytical", "r&d"],
        "specs": {"resolutionCm1": 0.09, "spectralRangeCm1": "7800-350", "detectorType": "DLaTGS"},
        "createdAt": dt("2023-06-01T10:00:00Z"), "updatedAt": dt("2025-11-30T14:00:00Z")
    },
    {
        "assetId": "ASSET-011", "name": "Robotic Arm RA-11", "type": "Robot",
        "category": "Automation", "manufacturer": "FANUC", "model": "M-20iD/25",
        "serialNumber": "FNC-2022-RA00711", "status": "operational",
        "description": "6-axis articulated robot arm used for PCB assembly and precision handling in R&D pilot line",
        "locationId": "LOC-004", "installDate": dt("2022-04-18T00:00:00Z"),
        "lastMaintenanceDate": dt("2026-04-01T00:00:00Z"),
        "tags": ["robot", "automation", "r&d", "pilot-line"],
        "specs": {"payloadKg": 25, "reachMm": 1831, "axes": 6, "repeatabilityMm": 0.02},
        "createdAt": dt("2022-04-18T10:00:00Z"), "updatedAt": dt("2026-04-01T11:30:00Z")
    },
    {
        "assetId": "ASSET-012", "name": "3D Printer DP-12", "type": "Printer",
        "category": "Laboratory Equipment", "manufacturer": "Stratasys", "model": "F370",
        "serialNumber": "STR-2023-FDM0122", "status": "operational",
        "description": "Industrial FDM 3D printer for rapid prototyping in R&D lab",
        "locationId": "LOC-004", "installDate": dt("2023-08-10T00:00:00Z"),
        "lastMaintenanceDate": dt("2025-08-10T00:00:00Z"),
        "tags": ["lab", "prototyping", "r&d"],
        "specs": {"buildVolumeXmm": 355, "buildVolumeYmm": 305, "buildVolumeZmm": 355,
                  "materialsSupported": ["ABS", "ASA", "PC", "Nylon"]},
        "createdAt": dt("2023-08-10T10:00:00Z"), "updatedAt": dt("2025-08-10T09:00:00Z")
    },
    {
        "assetId": "ASSET-013", "name": "Refrigerated Trailer RT-13", "type": "Vehicle",
        "category": "Logistics", "manufacturer": "Thermo King", "model": "Advancer A-500",
        "serialNumber": "TK-2021-REEFER113", "status": "offline",
        "description": "48-foot refrigerated trailer - currently out of service pending compressor overhaul",
        "locationId": "LOC-005", "installDate": dt("2021-01-10T00:00:00Z"),
        "lastMaintenanceDate": dt("2025-06-01T00:00:00Z"),
        "tags": ["vehicle", "cold-chain", "logistics"],
        "specs": {"lengthFt": 48, "tempRangeC": "-25 to +25", "refrigerantType": "R-404A"},
        "createdAt": dt("2021-01-10T10:00:00Z"), "updatedAt": dt("2025-06-01T10:00:00Z")
    },
    {
        "assetId": "ASSET-014", "name": "Dock Leveller DL-14", "type": "Dock Equipment",
        "category": "Facilities", "manufacturer": "Assa Abloy", "model": "Stertil Dock 12T",
        "serialNumber": "AA-2020-DOCK0047", "status": "operational",
        "description": "Hydraulic dock leveller at loading bay 4 - Miami hub",
        "locationId": "LOC-005", "installDate": dt("2020-10-20T00:00:00Z"),
        "lastMaintenanceDate": dt("2025-05-15T00:00:00Z"),
        "tags": ["dock", "hydraulic", "logistics"],
        "specs": {"capacityTonnes": 12, "platformWidthMm": 2000, "platformLengthMm": 2500},
        "createdAt": dt("2020-10-20T10:00:00Z"), "updatedAt": dt("2025-05-15T11:00:00Z")
    },
    {
        "assetId": "ASSET-015", "name": "Fire Suppression System FSS-15", "type": "Safety System",
        "category": "Safety", "manufacturer": "Kidde", "model": "FM-200 Total Flood",
        "serialNumber": "KDD-2019-FSS0015", "status": "operational",
        "description": "Clean-agent fire suppression system covering the server room and switchgear area",
        "locationId": "LOC-003", "installDate": dt("2019-08-01T00:00:00Z"),
        "lastMaintenanceDate": dt("2026-02-28T00:00:00Z"),
        "tags": ["safety", "critical", "fire-protection"],
        "specs": {"agent": "HFC-227ea", "coverageM3": 120, "activationDelaySeconds": 30},
        "createdAt": dt("2019-08-01T10:00:00Z"), "updatedAt": dt("2026-02-28T13:00:00Z")
    }
]

res = db["assets"].insert_many(assets)
print(f"Assets inserted:  {len(res.inserted_ids)}")

# ── Indexes ───────────────────────────────────────────────────────────────────
db["assets"].create_index([("assetId", ASCENDING)], unique=True, name="idx_assetId")
db["assets"].create_index([("locationId", ASCENDING)], name="idx_assets_locationId")
db["assets"].create_index([("status", ASCENDING), ("locationId", ASCENDING)], name="idx_assets_status_location")
db["assets"].create_index([("updatedAt", ASCENDING)], name="idx_assets_updatedAt")
db["locations"].create_index([("locationId", ASCENDING)], unique=True, name="idx_locationId")
db["locations"].create_index([("updatedAt", ASCENDING)], name="idx_locations_updatedAt")
print("Indexes created:  assets(4)  locations(2)")

# ── Verification ──────────────────────────────────────────────────────────────
print()
print(f"assetsdb.assets    : {db['assets'].count_documents({})} documents")
print(f"assetsdb.locations : {db['locations'].count_documents({})} documents")
print()

# Assets per location
pipeline = [
    {"$group": {"_id": "$locationId", "count": {"$sum": 1}}},
    {"$sort": {"_id": 1}}
]
print("Assets per location:")
for row in db["assets"].aggregate(pipeline):
    loc = db["locations"].find_one({"locationId": row["_id"]}, {"name": 1, "_id": 0})
    loc_name = loc["name"] if loc else "?"
    print(f"  {row['_id']}  ({loc_name})  ->  {row['count']} assets")

print()
# Non-operational assets
print("Non-operational assets:")
for a in db["assets"].find({"status": {"$ne": "operational"}},
                            {"assetId": 1, "name": 1, "status": 1, "locationId": 1, "_id": 0}):
    print(f"  {a['assetId']}  |  {a['name']}  |  {a['status']}  |  {a['locationId']}")

print()
# ASSET-011 quick check
a11 = db["assets"].find_one({"assetId": "ASSET-011"}, {"_id": 0})
print("ASSET-011:", a11)
