WITH victims_data AS (
  SELECT
    ocorrencias.id AS ocorrencia_id,
    address,
    state.id AS state_id,
    state.name AS state_name,
    city.id AS city_id,
    city.name AS city_name,
    latitude,
    longitude,
    TIMESTAMP(date) AS date,
    victims.age AS age,
    victims.ageGroup.id AS age_group_id,
    victims.ageGroup.name AS age_group_name,
    (SELECT AS STRUCT id, name, type FROM UNNEST([victims.agentPosition])) AS agent_position,
    (SELECT AS STRUCT id, name, type FROM UNNEST([victims.agentStatus])) AS agent_status,
    (SELECT AS STRUCT id, name, type FROM UNNEST([victims.politicalPosition])) AS political_position,
    (SELECT AS STRUCT id, name, type FROM UNNEST([victims.politicalStatus])) AS political_status,
    (SELECT AS STRUCT id, name, type FROM UNNEST([victims.serviceStatus])) AS service_status,
    (SELECT AS STRUCT id, name, type FROM UNNEST(victims.circumstances)) AS circumstances,
    victims.genre.id AS genre_id,
    victims.genre.name AS genre_name,
    victims.race AS race
  FROM
    encoded-density-405819.api_fogo_cruzado.ocorrencias AS ocorrencias,
    UNNEST(victims) AS victims
)

SELECT
  *
FROM
  victims_data;
