CREATE TABLE public.emergency_incident_stats (
    new_incidenttype varchar(50),
    incident_cnt BIGINT
);


SELECT * FROM public.emergency_incident_stats;

DROP TABLE public.emergency_incident_stats;