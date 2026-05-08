-- ============================================================
--  AVEVA DB  —  UniversalConnector seed
--  Runs automatically on first container start
-- ============================================================

CREATE DATABASE aveva_db;
\connect aveva_db

-- ── locations ────────────────────────────────────────────────────────────────
CREATE TABLE locations (
    id            SERIAL          PRIMARY KEY,
    location_id   VARCHAR(50)     NOT NULL UNIQUE,
    name          VARCHAR(200)    NOT NULL,
    type          VARCHAR(100),
    street        VARCHAR(200),
    city          VARCHAR(100),
    state         VARCHAR(50),
    country       VARCHAR(50),
    zip           VARCHAR(20),
    timezone      VARCHAR(60),
    active        BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_locations_location_id ON locations (location_id);
CREATE INDEX idx_locations_updated_at  ON locations (updated_at);

INSERT INTO locations (location_id, name, type, street, city, state, country, zip, timezone, active, created_at, updated_at) VALUES
('LOC-001', 'Houston Manufacturing Plant',   'Plant',      '1200 Industrial Blvd',    'Houston', 'TX', 'USA', '77001', 'America/Chicago',       TRUE,  '2023-01-15 08:00:00+00', '2025-11-20 10:00:00+00'),
('LOC-002', 'Denver Distribution Warehouse', 'Warehouse',  '4500 Logistics Parkway',  'Denver',  'CO', 'USA', '80201', 'America/Denver',         TRUE,  '2023-03-10 08:00:00+00', '2025-10-05 14:30:00+00'),
('LOC-003', 'Chicago Operations Center',     'Office',     '200 North Michigan Ave',  'Chicago', 'IL', 'USA', '60601', 'America/Chicago',        TRUE,  '2023-05-22 08:00:00+00', '2025-09-18 09:15:00+00'),
('LOC-004', 'Seattle R&D Facility',          'Laboratory', '9800 Innovation Drive',   'Seattle', 'WA', 'USA', '98101', 'America/Los_Angeles',    TRUE,  '2023-07-01 08:00:00+00', '2026-01-10 16:00:00+00'),
('LOC-005', 'Miami Distribution Hub',        'Warehouse',  '750 Port Boulevard',      'Miami',   'FL', 'USA', '33132', 'America/New_York',       FALSE, '2023-09-14 08:00:00+00', '2025-06-30 12:00:00+00');


-- ── assets ───────────────────────────────────────────────────────────────────
CREATE TABLE assets (
    id                    SERIAL          PRIMARY KEY,
    asset_id              VARCHAR(50)     NOT NULL UNIQUE,
    name                  VARCHAR(200)    NOT NULL,
    type                  VARCHAR(100),
    category              VARCHAR(100),
    manufacturer          VARCHAR(100),
    model                 VARCHAR(100),
    serial_number         VARCHAR(100),
    status                VARCHAR(50)     NOT NULL DEFAULT 'operational',
    description           TEXT,
    location_id           VARCHAR(50)     REFERENCES locations (location_id),
    install_date          DATE,
    last_maintenance_date DATE,
    tags                  TEXT[],
    specs                 JSONB,
    created_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_assets_asset_id         ON assets (asset_id);
CREATE INDEX idx_assets_location_id      ON assets (location_id);
CREATE INDEX idx_assets_status           ON assets (status);
CREATE INDEX idx_assets_updated_at       ON assets (updated_at);
CREATE INDEX idx_assets_specs_gin        ON assets USING GIN (specs);

INSERT INTO assets (asset_id, name, type, category, manufacturer, model, serial_number, status, description, location_id, install_date, last_maintenance_date, tags, specs, created_at, updated_at) VALUES
('ASSET-001', 'Main Feed Pump A',                  'Pump',           'Rotating Equipment',  'Flowserve',    'PVXM-220',          'FLW-2021-00441',   'operational', 'Primary centrifugal feed pump for Unit 1 crude distillation',                                     'LOC-001', '2021-03-15', '2025-09-10', ARRAY['critical','unit1','rotating'],         '{"powerKw":75,"flowRateLpm":1200,"maxPressureBar":16}',                                  '2021-03-15 10:00:00+00', '2025-09-10 08:30:00+00'),
('ASSET-002', 'Overhead Compressor B',             'Compressor',     'Rotating Equipment',  'Atlas Copco',  'GA-315',            'AC-2020-88712',    'operational', 'Reciprocating compressor for overhead gas recovery',                                              'LOC-001', '2020-07-20', '2025-11-01', ARRAY['critical','unit2','rotating'],         '{"powerKw":315,"capacityNm3h":4500,"maxPressureBar":25}',                                '2020-07-20 10:00:00+00', '2025-11-01 14:00:00+00'),
('ASSET-003', 'Shell & Tube Heat Exchanger E-101', 'Heat Exchanger', 'Static Equipment',    'Alfa Laval',   'STE-HXR-101',       'AL-2019-33210',    'maintenance', 'Pre-heat exchanger for crude feed - currently offline for tube bundle replacement',             'LOC-001', '2019-11-05', '2026-03-01', ARRAY['unit1','static','heat-transfer'],      '{"surfaceAreaM2":340,"shellSideBarG":12,"tubeSideBarG":8}',                              '2019-11-05 10:00:00+00', '2026-03-01 09:00:00+00'),
('ASSET-004', 'Control Valve FV-204',              'Valve',          'Instrumentation',     'Emerson',      'Fisher D4',         'EMR-2022-11045',   'operational', 'Flow control valve on distillate draw-off line',                                                  'LOC-001', '2022-01-10', '2025-08-20', ARRAY['instrumentation','unit1','control'],   '{"sizInch":4,"cvMax":120,"failPosition":"fail-close"}',                                  '2022-01-10 10:00:00+00', '2025-08-20 11:00:00+00'),
('ASSET-005', 'Pressure Transmitter PT-101',       'Sensor',         'Instrumentation',     'Yokogawa',     'EJA110E',           'YOK-2023-50012',   'operational', 'High-accuracy pressure transmitter on reactor inlet',                                             'LOC-001', '2023-02-14', '2025-12-05', ARRAY['sensor','unit1','pressure'],           '{"rangeBar":"0-40","accuracyPct":0.055,"outputSignal":"4-20mA HART"}',                   '2023-02-14 10:00:00+00', '2025-12-05 07:45:00+00'),
('ASSET-006', 'Forklift FL-06',                    'Vehicle',        'Material Handling',   'Toyota',       '8FBE20',            'TOY-2021-WH0892',  'operational', 'Electric counterbalance forklift assigned to Bay 3',                                              'LOC-002', '2021-06-01', '2026-01-15', ARRAY['vehicle','warehouse','bay3'],          '{"capacityKg":2000,"liftHeightMm":5000,"batteryVoltage":48}',                            '2021-06-01 10:00:00+00', '2026-01-15 08:00:00+00'),
('ASSET-007', 'Conveyor Belt CB-07',               'Conveyor',       'Material Handling',   'Hytrol',       'EZLogic ZPA',       'HYT-2022-44500',   'operational', 'Zero-pressure accumulation conveyor for outbound sorting area',                                   'LOC-002', '2022-09-15', '2025-10-20', ARRAY['conveyor','warehouse','sorting'],      '{"lengthM":45,"widthMm":600,"speedMpm":30}',                                             '2022-09-15 10:00:00+00', '2025-10-20 13:00:00+00'),
('ASSET-008', 'HVAC Unit AHU-08',                  'HVAC',           'Facilities',          'Carrier',      'WeatherMaster 50XC','CAR-2020-CLI1023', 'operational', 'Air handling unit for main office floor 3',                                                       'LOC-003', '2020-04-10', '2025-07-12', ARRAY['facilities','hvac','floor3'],          '{"coolingTonnes":20,"airflowCfm":8000,"refrigerant":"R-410A"}',                          '2020-04-10 10:00:00+00', '2025-07-12 10:30:00+00'),
('ASSET-009', 'UPS System UPS-09',                 'Power',          'Electrical',          'Eaton',        '9395P-1100',        'ETN-2021-UPS7741', 'operational', '1.1 MVA UPS protecting data centre and critical servers',                                         'LOC-003', '2021-11-20', '2026-02-14', ARRAY['electrical','critical','datacenter'],  '{"powerKva":1100,"runtimeMinutes":15,"topology":"double-conversion"}',                   '2021-11-20 10:00:00+00', '2026-02-14 09:00:00+00'),
('ASSET-010', 'Spectrometer SP-10',                'Analyser',       'Laboratory Equipment','Thermo Fisher','Nicolet iS50',      'TF-2023-FTIR0044', 'operational', 'FT-IR spectrometer for polymer and material characterization',                                    'LOC-004', '2023-06-01', '2025-11-30', ARRAY['lab','analytical','r&d'],             '{"resolutionCm1":0.09,"spectralRangeCm1":"7800-350","detectorType":"DLaTGS"}',           '2023-06-01 10:00:00+00', '2025-11-30 14:00:00+00'),
('ASSET-011', 'Robotic Arm RA-11',                 'Robot',          'Automation',          'FANUC',        'M-20iD/25',         'FNC-2022-RA00711', 'operational', '6-axis articulated robot arm used for PCB assembly and precision handling in R&D pilot line',    'LOC-004', '2022-04-18', '2026-04-01', ARRAY['robot','automation','r&d','pilot-line'],'{"payloadKg":25,"reachMm":1831,"axes":6,"repeatabilityMm":0.02}',                       '2022-04-18 10:00:00+00', '2026-04-01 11:30:00+00'),
('ASSET-012', '3D Printer DP-12',                  'Printer',        'Laboratory Equipment','Stratasys',    'F370',              'STR-2023-FDM0122', 'operational', 'Industrial FDM 3D printer for rapid prototyping in R&D lab',                                      'LOC-004', '2023-08-10', '2025-08-10', ARRAY['lab','prototyping','r&d'],             '{"buildVolumeXmm":355,"buildVolumeYmm":305,"buildVolumeZmm":355,"materialsSupported":["ABS","ASA","PC","Nylon"]}', '2023-08-10 10:00:00+00', '2025-08-10 09:00:00+00'),
('ASSET-013', 'Refrigerated Trailer RT-13',        'Vehicle',        'Logistics',           'Thermo King',  'Advancer A-500',    'TK-2021-REEFER113','offline',     '48-foot refrigerated trailer - currently out of service pending compressor overhaul',            'LOC-005', '2021-01-10', '2025-06-01', ARRAY['vehicle','cold-chain','logistics'],    '{"lengthFt":48,"tempRangeC":"-25 to +25","refrigerantType":"R-404A"}',                   '2021-01-10 10:00:00+00', '2025-06-01 10:00:00+00'),
('ASSET-014', 'Dock Leveller DL-14',               'Dock Equipment', 'Facilities',          'Assa Abloy',   'Stertil Dock 12T',  'AA-2020-DOCK0047', 'operational', 'Hydraulic dock leveller at loading bay 4 - Miami hub',                                            'LOC-005', '2020-10-20', '2025-05-15', ARRAY['dock','hydraulic','logistics'],        '{"capacityTonnes":12,"platformWidthMm":2000,"platformLengthMm":2500}',                   '2020-10-20 10:00:00+00', '2025-05-15 11:00:00+00'),
('ASSET-015', 'Fire Suppression System FSS-15',    'Safety System',  'Safety',              'Kidde',        'FM-200 Total Flood', 'KDD-2019-FSS0015','operational', 'Clean-agent fire suppression system covering the server room and switchgear area',              'LOC-003', '2019-08-01', '2026-02-28', ARRAY['safety','critical','fire-protection'], '{"agent":"HFC-227ea","coverageM3":120,"activationDelaySeconds":30}',                     '2019-08-01 10:00:00+00', '2026-02-28 13:00:00+00');
