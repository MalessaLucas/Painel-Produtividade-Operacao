CREATE OR REPLACE VIEW hive_metastore.dev_lucas_malessa.vw_fact_aprovacao AS 
WITH
pi_logs AS (
  SELECT
    dmplv.ClientId AS CodigoCliente,
    CAST(dmplv.ParameterLogOrgValueModificationTimestamp AS TIMESTAMP) AS ts,
    COALESCE(dmplv.OldValueDescription, '') AS old_values,
    COALESCE(dmplv.NewValueDescription, '') AS new_values
  FROM hive_metastore.gold.Dim_MaintenanceParameterLogValue AS dmplv
  WHERE dmplv.ParameterId = 586
)
, pi_sets AS (
  SELECT
    CodigoCliente, ts,
    array_distinct(filter(split(regexp_replace(old_values, '\\\\s+', ''), '[;,]+'), x -> x <> '')) AS old_set,
    array_distinct(filter(split(regexp_replace(new_values, '\\\\s+', ''), '[;,]+'), x -> x <> '')) AS new_set
  FROM pi_logs
)
, pi_adds AS (
  SELECT CodigoCliente, ts, CAST(approver AS STRING) AS approver
  FROM pi_sets LATERAL VIEW explode(new_set) ns AS approver
  WHERE NOT array_contains(old_set, approver)
)
, pi_removes AS (
  SELECT CodigoCliente, ts, CAST(approver AS STRING) AS approver
  FROM pi_sets LATERAL VIEW explode(old_set) os AS approver
  WHERE NOT array_contains(new_set, approver)
)
, pi_events AS (
  SELECT CodigoCliente, approver, ts, 'ADD' AS event_type FROM pi_adds
  UNION ALL
  SELECT CodigoCliente, approver, ts, 'REMOVE' AS event_type FROM pi_removes
)
, pi_ordered AS (
  SELECT CodigoCliente, approver, event_type, ts,
    LEAD(ts) OVER (PARTITION BY CodigoCliente, approver ORDER BY ts) AS next_ts,
    LEAD(event_type) OVER (PARTITION BY CodigoCliente, approver ORDER BY ts) AS next_event_type
  FROM pi_events
)
, param_intervals AS (
  SELECT CodigoCliente, approver AS Aprovador, dwu.WebUserName AS nome_aprovador,
    CAST(date_format(ts, 'yyyy-MM-dd HH:mm') AS STRING) AS DataInicio,
    CAST(date_format(next_ts, 'yyyy-MM-dd HH:mm') AS STRING) AS DataFim,
    CAST(ts AS DATE) AS DataInicioDate,
    CAST(COALESCE(next_ts, TIMESTAMP '9999-12-31 23:59:59') AS DATE) AS DataFimDate
  FROM pi_ordered
  LEFT JOIN hive_metastore.gold.dim_webusers AS dwu ON dwu.WebUserSourceCode = pi_ordered.approver
  WHERE event_type = 'ADD'
)
, CTE_BASE AS (
  SELECT DISTINCT fms.OrderServiceCode
    , CASE WHEN (aprov1.IsInternalUser = TRUE OR aprov2.IsInternalUser = TRUE OR aprov3.IsInternalUser = TRUE) THEN TRUE ELSE FALSE END AS OS_Aprovada
    , fms.IsExternalApproval
    , dmv.CustomerId
    
    -- OTIMIZACAO P2: Convertendo colunas de DateTime para Date
    , CAST(CASE WHEN aprov1.IsInternalUser = TRUE THEN (fms.FistBudgetSentTimestamp) WHEN aprov2.IsInternalUser = TRUE THEN (fms.FirstApprovalTimestamp) WHEN aprov3.IsInternalUser = TRUE THEN (fms.SecondApprovalTimestamp) ELSE NULL END AS DATE) AS _data_sla1
    , CAST(CASE WHEN aprov1.IsInternalUser = TRUE THEN (fms.FirstApprovalTimestamp) WHEN aprov2.IsInternalUser = TRUE THEN (fms.SecondApprovalTimestamp) WHEN aprov3.IsInternalUser = TRUE THEN (fms.ThirdApprovalTimestamp) ELSE NULL END AS DATE) AS _data_sla2
    
    , dev_charles_barros.fn_calcular_sla_formatado(
        CASE WHEN aprov1.IsInternalUser = TRUE THEN (fms.FistBudgetSentTimestamp) WHEN aprov2.IsInternalUser = TRUE THEN (fms.FirstApprovalTimestamp) WHEN aprov3.IsInternalUser = TRUE THEN (fms.SecondApprovalTimestamp) ELSE NULL END,
        CASE WHEN aprov1.IsInternalUser = TRUE THEN (fms.FirstApprovalTimestamp) WHEN aprov2.IsInternalUser = TRUE THEN (fms.SecondApprovalTimestamp) WHEN aprov3.IsInternalUser = TRUE THEN (fms.ThirdApprovalTimestamp) ELSE NULL END,
        1) AS sla_aprovocao
    , CASE WHEN aprov1.IsInternalUser = TRUE THEN DATE(fms.FirstApprovalTimestamp) WHEN aprov2.IsInternalUser = TRUE THEN DATE(fms.SecondApprovalTimestamp) WHEN aprov3.IsInternalUser = TRUE THEN DATE(fms.ThirdApprovalTimestamp) WHEN reprov.IsInternalUser = TRUE THEN DATE(fms.DisapprovalTimestamp) ELSE NULL END AS data_aprovacao
    , CASE WHEN aprov1.IsInternalUser = TRUE THEN aprov1.WebUserName WHEN aprov2.IsInternalUser = TRUE THEN aprov2.WebUserName WHEN aprov3.IsInternalUser = TRUE THEN aprov3.WebUserName WHEN reprov.IsInternalUser = TRUE THEN reprov.WebUserName ELSE NULL END AS _aprovador
    , CASE WHEN (dmv.maintenancemodelfamily IS NULL OR dmv.maintenancemodelfamily = 'Nao Identificado' OR dmv.maintenancemodelfamily = 'Nao Definido') THEN (CASE WHEN (dmv.familycategory IS NULL OR dmv.familycategory = 'Nao Identificado') THEN 'Leve' ELSE dmv.familycategory END) ELSE dmv.maintenancemodelfamily END AS familia_do_modelo
    , SUM(fmi.QuotedPrice) AS _valor_os
    , SUM(fmi.PriceApproved) AS _valor_aprovado
    , CASE WHEN fms.MaintenanceTypeDescription = 'Sinistro' THEN 'Sinistro' WHEN dmt.MaintenanceType = 'Preventiva' THEN 'Preventiva' ELSE 'Corretiva' END AS _tipo_manutencao
    , CASE WHEN COUNT(CASE WHEN fmi.CancellationTimestamp IS NULL THEN fmi.Sk_MaintenanceItem END) <= 03 THEN '01-03' WHEN COUNT(CASE WHEN fmi.CancellationTimestamp IS NULL THEN fmi.Sk_MaintenanceItem END) <= 05 THEN '04-05' WHEN COUNT(CASE WHEN fmi.CancellationTimestamp IS NULL THEN fmi.Sk_MaintenanceItem END) <= 07 THEN '06-07' WHEN COUNT(CASE WHEN fmi.CancellationTimestamp IS NULL THEN fmi.Sk_MaintenanceItem END) <= 10 THEN '08-10' WHEN COUNT(CASE WHEN fmi.CancellationTimestamp IS NULL THEN fmi.Sk_MaintenanceItem END) <= 20 THEN '11-20' WHEN COUNT(CASE WHEN fmi.CancellationTimestamp IS NULL THEN fmi.Sk_MaintenanceItem END) <= 30 THEN '21-30' WHEN COUNT(CASE WHEN fmi.CancellationTimestamp IS NULL THEN fmi.Sk_MaintenanceItem END) <= 40 THEN '31-40' WHEN COUNT(CASE WHEN fmi.CancellationTimestamp IS NULL THEN fmi.Sk_MaintenanceItem END) <= 50 THEN '41-50' ELSE '50 ou mais' END AS _faixa_qtde_itens
    , CAST(CASE WHEN _tipo_manutencao = 'Corretiva' AND _faixa_qtde_itens = '01-03' THEN 8 WHEN _tipo_manutencao = 'Corretiva' AND _faixa_qtde_itens = '04-05' THEN 12 WHEN _tipo_manutencao = 'Corretiva' AND _faixa_qtde_itens = '06-07' THEN 14 WHEN _tipo_manutencao = 'Corretiva' AND _faixa_qtde_itens = '08-10' THEN 20 WHEN _tipo_manutencao = 'Corretiva' AND _faixa_qtde_itens = '11-20' THEN 25 WHEN _tipo_manutencao = 'Corretiva' AND _faixa_qtde_itens = '21-30' THEN 35 WHEN _tipo_manutencao = 'Corretiva' AND _faixa_qtde_itens = '31-40' THEN 45 WHEN _tipo_manutencao = 'Corretiva' AND _faixa_qtde_itens = '41-50' THEN 55 WHEN _tipo_manutencao = 'Corretiva' AND _faixa_qtde_itens = '50 ou mais' THEN 65 WHEN _tipo_manutencao = 'Preventiva' AND _faixa_qtde_itens = '01-03' THEN 6 WHEN _tipo_manutencao = 'Preventiva' AND _faixa_qtde_itens = '04-05' THEN 8 WHEN _tipo_manutencao = 'Preventiva' AND _faixa_qtde_itens = '06-07' THEN 10 WHEN _tipo_manutencao = 'Preventiva' AND _faixa_qtde_itens = '08-10' THEN 14 WHEN _tipo_manutencao = 'Preventiva' AND _faixa_qtde_itens = '11-20' THEN 18 WHEN _tipo_manutencao = 'Preventiva' AND _faixa_qtde_itens = '21-30' THEN 25 WHEN _tipo_manutencao = 'Preventiva' AND _faixa_qtde_itens = '31-40' THEN 45 WHEN _tipo_manutencao = 'Preventiva' AND _faixa_qtde_itens = '41-50' THEN 55 WHEN _tipo_manutencao = 'Preventiva' AND _faixa_qtde_itens = '50 ou mais' THEN 65 WHEN _tipo_manutencao = 'Sinistro' AND _faixa_qtde_itens = '01-03' THEN 12 WHEN _tipo_manutencao = 'Sinistro' AND _faixa_qtde_itens = '04-05' THEN 18 WHEN _tipo_manutencao = 'Sinistro' AND _faixa_qtde_itens = '06-07' THEN 21 WHEN _tipo_manutencao = 'Sinistro' AND _faixa_qtde_itens = '08-10' THEN 30 WHEN _tipo_manutencao = 'Sinistro' AND _faixa_qtde_itens = '11-20' THEN 38 WHEN _tipo_manutencao = 'Sinistro' AND _faixa_qtde_itens = '21-30' THEN 53 WHEN _tipo_manutencao = 'Sinistro' AND _faixa_qtde_itens = '31-40' THEN 68 WHEN _tipo_manutencao = 'Sinistro' AND _faixa_qtde_itens = '41-50' THEN 83 WHEN _tipo_manutencao = 'Sinistro' AND _faixa_qtde_itens = '50 ou mais' THEN 98 ELSE NULL END AS INT) AS tempo_padrao
    , CASE WHEN dmt.MaintenanceType LIKE '%Preventiva%' THEN 'Preventiva' WHEN familia_do_modelo IN ('Caminhao Pesado','Pesado','Cavalo','Trator Agricola') AND _valor_os > 35000 OR familia_do_modelo IN ('Caminhao','Implemento','Onibus','Micro Onibus','Reboque') AND _valor_os > 15000 OR familia_do_modelo IN ('Pickup Pesada','Equipamento','Suv') AND _valor_os > 15000 OR familia_do_modelo = 'Equipamento' AND _valor_os > 15000 OR familia_do_modelo = 'Pickup Media' AND _valor_os > 10000 OR familia_do_modelo = 'Pickup Leve' AND _valor_os > 7000 OR familia_do_modelo IN ('Van','Utilitario') AND _valor_os > 15000 OR familia_do_modelo = 'Leve' AND _valor_os > 6000 OR familia_do_modelo IN ('Moto','Quadricilo') AND _valor_os > 2000 THEN 'alta_monta' WHEN familia_do_modelo IN ('Caminhao Pesado','Pesado','Cavalo','Trator Agricola') AND _valor_os <= 35000 AND _valor_os >= 15000 OR familia_do_modelo IN ('Caminhao','Implemento','Onibus','Micro Onibus','Reboque') AND _valor_os <= 15000 AND _valor_os >= 5000 OR familia_do_modelo IN ('Pickup Pesada','Equipamento','Suv') AND _valor_os <= 15000 AND _valor_os >= 5000 OR familia_do_modelo = 'Equipamento' AND _valor_os <= 15000 AND _valor_os >= 5000 OR familia_do_modelo = 'Pickup Media' AND _valor_os <= 10000 AND _valor_os >= 3000 OR familia_do_modelo = 'Pickup Leve' AND _valor_os <= 7000 AND _valor_os >= 2000 OR familia_do_modelo IN ('Van','Utilitario') AND _valor_os <= 15000 AND _valor_os >= 5000 OR familia_do_modelo = 'Leve' AND _valor_os <= 6000 AND _valor_os >= 1500 OR familia_do_modelo IN ('Moto','Quadricilo') AND _valor_os <= 2000 AND _valor_os >= 600 THEN 'media_monta' WHEN familia_do_modelo IN ('Caminhao Pesado','Pesado','Cavalo','Trator Agricola') AND _valor_os < 15000 OR familia_do_modelo IN ('Caminhao','Implemento','Onibus','Micro Onibus','Reboque') AND _valor_os < 5000 OR familia_do_modelo IN ('Pickup Pesada','Equipamento','Suv') AND _valor_os < 5000 OR familia_do_modelo = 'Equipamento' AND _valor_os < 5000 OR familia_do_modelo = 'Pickup Media' AND _valor_os < 3000 OR familia_do_modelo = 'Pickup Leve' AND _valor_os < 2000 OR familia_do_modelo IN ('Van','Utilitario') AND _valor_os < 5000 OR familia_do_modelo = 'Leve' AND _valor_os < 1500 OR familia_do_modelo IN ('Moto','Quadricilo') AND _valor_os < 600 THEN 'baixa_monta' ELSE 'alta_monta' END AS _monta
    , CASE WHEN _monta = 'baixa_monta' THEN 1 WHEN _monta = 'media_monta' THEN 1.3 WHEN _monta = 'alta_monta' THEN 1.5 WHEN _monta = 'Preventiva' THEN 1 ELSE NULL END AS fator_correcao_monta
    , moh.total_revisao AS total_int_rev
    , moh.total_cotacao AS total_int_cot
    , (total_int_rev + total_int_cot) AS total_interacoes
    , CASE WHEN (total_int_rev >= 1) THEN (total_int_rev * 5) ELSE 0 END AS tempo_extra_revisao
    , CASE WHEN (dmv.CustomerId = 233898 AND (total_int_cot) >= 3) THEN 15 WHEN (dmv.CustomerId = 233898 AND (total_int_cot) < 3 AND (total_int_cot) >= 1) THEN (total_int_cot * 5) WHEN (total_int_cot >= 1) THEN 5 ELSE 0 END AS tempo_extra_cotacao
    , (tempo_extra_revisao + tempo_extra_cotacao) AS tempo_extra_interacao
    , CASE WHEN dmv.CustomerId IN (230011, 232129, 233980, 233985, 235548, 235550, 235552, 240752, 231037, 231038, 231039, 231040, 231048, 231051, 231052, 231055, 234455, 234456, 234913, 187966) THEN COUNT(CASE WHEN fmi.CancellationTimestamp IS NULL THEN fmi.Sk_MaintenanceItem END) + 5 WHEN dmv.CustomerId = 233898 THEN (COUNT(CASE WHEN fmi.CancellationTimestamp IS NULL THEN fmi.Sk_MaintenanceItem END) * 1.5) + 5 ELSE 0 END AS tempo_extra_mercadopublico
    , dmsost.StatusTypeDescription
  FROM hive_metastore.gold.fact_maintenanceservices AS fms
  LEFT JOIN hive_metastore.gold.dim_maintenanceserviceorderstatustypes AS dmsost ON fms.Sk_ServiceOrderStatusType = dmsost.Sk_ServiceOrderStatusType
  LEFT JOIN hive_metastore.gold.dim_maintenancevehicles AS dmv ON fms.Sk_MaintenanceVehicle = dmv.Sk_MaintenanceVehicle
  LEFT JOIN hive_metastore.gold.dim_maintenancetypes AS dmt ON fms.Sk_MaintenanceType = dmt.Sk_MaintenanceType
  LEFT JOIN hive_metastore.gold.dim_Maintenanceprotocols AS dmp ON fms.OrderServiceCode = dmp.ProtocolSourceCode
  LEFT JOIN hive_metastore.gold.fact_maintenanceattendances AS fma ON dmp.Sk_MaintenanceProtocol = fma.Sk_MaintenanceProtocol
  LEFT JOIN hive_metastore.gold.Fact_MaintenanceItems AS fmi ON fms.OrderServiceCode = fmi.MaintenanceId
  LEFT JOIN hive_metastore.gold.dim_webusers AS aprov1 ON fms.Sk_FirstApprover = aprov1.Sk_WebUser
  LEFT JOIN hive_metastore.gold.dim_webusers AS aprov2 ON fms.Sk_SecondApprover = aprov2.Sk_WebUser
  LEFT JOIN hive_metastore.gold.dim_webusers AS aprov3 ON fms.Sk_ThirdApprover = aprov3.Sk_WebUser
  LEFT JOIN hive_metastore.gold.dim_webusers AS reprov ON fms.Sk_ServiceOrderApprover = reprov.Sk_WebUser
  LEFT JOIN hive_metastore.gold.dim_dates AS dd ON (CASE WHEN aprov1.IsInternalUser = TRUE THEN DATE(fms.FirstApprovalTimestamp) WHEN aprov2.IsInternalUser = TRUE THEN DATE(fms.SecondApprovalTimestamp) WHEN aprov3.IsInternalUser = TRUE THEN DATE(fms.ThirdApprovalTimestamp) WHEN reprov.IsInternalUser = TRUE THEN DATE(fms.DisapprovalTimestamp) ELSE NULL END) = dd.ReferenceDate
  LEFT JOIN (SELECT DISTINCT cd_manutencao_oficina, COUNT(DISTINCT CASE WHEN cd_evento_manutencao_oficina = 11 THEN date_format(dt_modificacao, 'yyyy-MM-dd HH:mm') END) AS total_revisao, COUNT(DISTINCT CASE WHEN cd_evento_manutencao_oficina = 14 THEN date_format(dt_modificacao, 'yyyy-MM-dd HH:mm') END) AS total_cotacao FROM hive_metastore.bronze.tlbagda_fuel_manutencao_oficina_historico WHERE cd_evento_manutencao_oficina IN (11,14) GROUP BY cd_manutencao_oficina) AS moh ON fms.OrderServiceCode = moh.cd_manutencao_oficina
  WHERE 1=1
    AND dd.ReferenceDate >= '2025-01-01'
    AND (dd.HolidayOrBridge IS NULL OR dd.HolidayOrBridge = 'Bridge')
    -- OTIMIZACAO P1 (Sabados): REGRAS ANTERIORES HARDCODED FORAM REMOVIDAS AQUI.
    AND (aprov1.IsInternalUser = TRUE OR aprov2.IsInternalUser = TRUE OR aprov3.IsInternalUser = TRUE OR reprov.IsInternalUser = TRUE)
    AND fms.IsAutomaticApproval = FALSE
  GROUP BY fms.OrderServiceCode, aprov1.IsInternalUser, aprov2.IsInternalUser, aprov3.IsInternalUser, fms.IsExternalApproval, _aprovador, _tipo_manutencao, familia_do_modelo, dmt.MaintenanceType, data_aprovacao, total_revisao, total_cotacao, _data_sla1, _data_sla2, sla_aprovocao, dmsost.StatusTypeDescription, dmv.CustomerId
)
, CTE_FLAG AS (
  SELECT b.*, CASE WHEN pi.Aprovador IS NOT NULL THEN TRUE ELSE FALSE END AS preco_parceiro
  FROM CTE_BASE b
  LEFT JOIN param_intervals pi ON pi.CodigoCliente = b.CustomerId AND pi.nome_aprovador = b._aprovador AND b.data_aprovacao BETWEEN pi.DataInicioDate AND pi.DataFimDate
)
, CTE_CALC AS (
  SELECT f.*, CASE WHEN f.preco_parceiro THEN 1.1 ELSE 1 END AS fator_correcao_precoparceiro,
    (7.48*60 / (f.tempo_padrao + f.tempo_extra_interacao + f.tempo_extra_mercadopublico)) / (f.fator_correcao_monta * (CASE WHEN f.preco_parceiro THEN 1.1 ELSE 1 END)) AS meta_os_dia_corrigido,
    ROUND(1 / ((7.48*60 / (f.tempo_padrao + f.tempo_extra_interacao + f.tempo_extra_mercadopublico)) / (f.fator_correcao_monta * (CASE WHEN f.preco_parceiro THEN 1.1 ELSE 1 END))), 2) AS peso_os
  FROM CTE_FLAG f
)
, CTE2 AS (
  SELECT ddt.YearWeek, CTE_CALC._aprovador, COUNT(DISTINCT CTE_CALC.data_aprovacao) AS total_dias_uteis_semana
  FROM CTE_CALC
  LEFT JOIN hive_metastore.gold.dim_dates AS ddt ON ddt.ReferenceDate = CTE_CALC.data_aprovacao
  WHERE ddt.YearWeek IS NOT NULL AND (ddt.HolidayOrBridge IS NULL OR ddt.HolidayOrBridge = 'Bridge') AND ddt.WeekDayNumber <= 4
  GROUP BY ddt.YearWeek, CTE_CALC._aprovador
)
, CTE3 AS (
  SELECT fmi.Sk_MaintenanceItem, fmi.MaintenanceId, lab.LaborName, fmi.PartReferencePrice, fmi.PartPriceReferenceCustomer, fmi.PartPriceNegociated, fmi.PartPriceNegociatedCustomer,
    COALESCE(fmi.PartPriceNegociatedCustomer, fmi.PartPriceNegociated, fmi.PartPriceReferenceCustomer, fmi.PartReferencePrice) AS PrecoReferencial,
    fmi.PartUnitaryPrice, fmi.PartQuantity, fmi.PartPriceApproved,
    CASE WHEN PrecoReferencial >= fmi.PartUnitaryPrice THEN fmi.PartPriceApproved ELSE NULL END AS Aderente
  FROM hive_metastore.gold.fact_maintenanceitems AS fmi
  LEFT JOIN hive_metastore.gold.dim_maintenancelabors AS lab ON fmi.Sk_MaintenanceLabor = lab.Sk_MaintenanceLabor
  LEFT JOIN hive_metastore.gold.fact_maintenanceservices AS fms ON fmi.MaintenanceId = fms.OrderServiceCode
  LEFT JOIN hive_metastore.gold.dim_maintenancemerchants AS dmm ON fms.Sk_MaintenanceMerchant = dmm.Sk_MaintenanceMerchant
  INNER JOIN CTE_CALC ON CTE_CALC.OrderServiceCode = fmi.MaintenanceId
  WHERE fms.IsAutomaticApproval = FALSE AND fmi.CancellationTimestamp IS NULL
    AND lab.LaborName IN ('SUBSTITUIR','SUBSTITUIR COM REVISAO CUBO','SUBSTITUIR SEM REVISAO CUBO','FORNECIMENTO DE PECAS')
    AND fmi.PartQuantity >= 1 AND fmi.PartUnitaryPrice > 0.1 AND dmm.NameMerchantsTypes <> 'Concessionaria'
    AND COALESCE(fmi.PartPriceNegociatedCustomer, fmi.PartPriceNegociated, fmi.PartPriceReferenceCustomer, fmi.PartReferencePrice) IS NOT NULL
)
, CTE4 AS (
  SELECT DISTINCT MaintenanceId, SUM(PartPriceApproved) AS total_aprovado_referencial, SUM(Aderente) AS total_aprovado_aderente
  FROM CTE3 GROUP BY MaintenanceId
)
SELECT CTE_CALC._aprovador, OrderServiceCode, CTE_CALC.OS_Aprovada, CustomerId, IsExternalApproval,
  _valor_os AS valor_orcado, _valor_aprovado, CTE4.total_aprovado_referencial, CTE4.total_aprovado_aderente,
  data_aprovacao, ddt.YearWeek, sla_aprovocao, _data_sla1, _data_sla2, _tipo_manutencao, _faixa_qtde_itens,
  familia_do_modelo, _monta, total_int_rev, total_int_cot, peso_os, StatusTypeDescription, CTE_CALC.preco_parceiro,
  SUM(peso_os) OVER (PARTITION BY ddt.YearWeek, CTE_CALC._aprovador) AS soma_peso_os_semana_aprovador,
  COUNT(OrderServiceCode) OVER (PARTITION BY ddt.YearWeek, CTE_CALC._aprovador) AS total_os_semana_aprovador,
  CTE2.total_dias_uteis_semana,
  (total_os_semana_aprovador/total_dias_uteis_semana) AS media_diaria_os_aprovador,
  (soma_peso_os_semana_aprovador/total_dias_uteis_semana) AS produtividade_semana_aprovador
FROM CTE_CALC
LEFT JOIN hive_metastore.gold.dim_dates AS ddt ON ddt.ReferenceDate = CTE_CALC.data_aprovacao
LEFT JOIN CTE2 ON CTE2.YearWeek = ddt.YearWeek AND CTE2._aprovador = CTE_CALC._aprovador
LEFT JOIN CTE4 ON CTE4.MaintenanceId = CTE_CALC.OrderServiceCode
