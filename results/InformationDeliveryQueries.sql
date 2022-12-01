SELECT
fo."ObservationMetaDataKey",
fo."ObservationKey",
dob."name",
dob."value",
dob."timestamps"
FROM "FactObservation" fo 
INNER JOIN "DimObservation" dob ON (
    fo."ObservationKey" = dob."ObservationKey"
) WHERE dob."name" like '%ViMo%' and dob."name" like '%Oxy%'

SELECT 
fo."ObservationMetaDataKey",
fo."ObservationKey",
dob."name",
dob."value",
dob."timestamps"
FROM "FactObservation" fo 
INNER JOIN "DimObservation" dob ON (
    fo."ObservationKey" = dob."ObservationKey"
) WHERE dob."name" like '%ViMo%' and dob."name" like '%Deoxy%'

SELECT 
fo."ObservationMetaDataKey",
fo."ObservationKey",
dob."name",
dob."value"[1:array_upper(dob."value", 1)][1:2],
dob."timestamps"
FROM "FactObservation" fo 
INNER JOIN "DimObservation" dob ON (
    fo."ObservationKey" = dob."ObservationKey"
) WHERE dob."name" like '%Viso%' and dob."name" like '%Oxy%'

SELECT 
fo."ObservationMetaDataKey",
fo."ObservationKey",
dob."name",
dob."value"[1:array_upper(dob."value", 1)][1:1],
dob."timestamps"
FROM "FactObservation" fo 
INNER JOIN "DimObservation" dob ON (
    fo."ObservationKey" = dob."ObservationKey"
) WHERE dob."name" = 'VM0001_Moto_HBA_Probe1_Deoxy'
					
SELECT 
fo."ObservationMetaDataKey",
fo."ObservationKey",
dob."name",
dob."value"[1:array_upper(dob."value", 1)][1:2],
dob."timestamps"
FROM "FactObservation" fo 
INNER JOIN "DimObservation" dob ON (
    fo."ObservationKey" = dob."ObservationKey"
) WHERE dob."name" = 'VM0001_Moto_HBA_Probe1_Deoxy'
					
select 
    dmd."key",
    dmd."value"
from "FactObservation" fo
INNER JOIN "DimMetaData" dmd ON (
    dmd."MetaDataKey" = fo."MetaDataKey"
)
INNER JOIn "DimObservation" dob ON (
    dob."ObservationKey" = fo."ObservationKey"
) WHERE dob."name" = 'VM0001_Moto_HBA_Probe1_Deoxy'
					
select 
    de."title",
    df."name",
    df."levelValue"
from "FactTreatmentFactors" ftf
INNER JOIN "DimExperiment" de ON (
    de."ExperimentKey" = ftf."Experimentkey"
)
INNER JOIN "DimFactor" df ON (
    df."FactorKey" = ftf."Factorkey"
)

select 
    dg."name",
    ds."name"
from "FactAssignedTo" fat 
INNER JOIN "DimGroup" dg ON (
    dg."GroupKey" = fat."GroupKey"
)
INNER JOIN "DimSubject" ds ON (
    fat."ExperimentalUnitKey" = ds."SubjectKey"
) WHERE ds."name" LIKE 'Subj%'
					
select 
    dg."name",
    ds."name"
from "FactAssignedTo" fat 
INNER JOIN "DimGroup" dg ON (
    dg."GroupKey" = fat."GroupKey"
)
INNER JOIN "DimSubject" ds ON (
    fat."ExperimentalUnitKey" = ds."SubjectKey"
) WHERE ds."name" LIKE 'Autism%'

