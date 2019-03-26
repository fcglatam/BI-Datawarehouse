WITH
-- Unir el QC score con car_id y nombre de inspectio
QCScore AS (
SELECT MXDT.car_id
, OG_ID.inspection_qc_score
, CASE WHEN inspector IS NULL THEN 'Desconocido' 
  ELSE UPPER(LEFT(inspector, 1))
    + SUBSTRING(inspector, 2, CHARINDEX('.', inspector) - 2) + ' '
    + UPPER(LEFT(SUBSTRING(inspector, 
        CHARINDEX('.', inspector) + 1, 
        LEN(inspector) - CHARINDEX('.', inspector)), 1))
    + SUBSTRING(inspector, 
        CHARINDEX('.', inspector) + 2, 
        CHARINDEX('@', inspector) - CHARINDEX('.',inspector) - 2) END AS inspector
FROM Inspections INSP
LEFT JOIN (SELECT original_inspection_id
  , inspection_qc_score
  FROM Inspections) OG_ID
  ON INSP.inspection_id = OG_ID.original_inspection_id
LEFT JOIN (
      SELECT car_id
      , MAX(inspection_date) AS inspection_date 
      FROM Inspections 
      WHERE car_id IS NOT NULL 
      GROUP BY car_id) MXDT
  ON MXDT.car_id = INSP.car_id 
  AND MXDT.inspection_date = INSP.inspection_date
)

, LastWinnerAuction AS (
  SELECT car_id
  , CAST(auction_end_date AS DATE) AS auction_last_date
  , CASE WHEN DATEPART(HOUR, auction_start_date) < 14
      THEN 'AM' ELSE 'PM' END AS auction_class 
  , auction_start_price
  , auction_buy_now_price
  FROM (SELECT auction_id
      , car_id 
      , auction_start_date 
      , auction_end_date
      , auction_buy_now_price 
      , auction_start_price
      , auc_row = ROW_NUMBER() 
          OVER (PARTITION BY car_id ORDER BY auction_start_date DESC)
    FROM Auctions AUC
    LEFT JOIN AuctionStatus STT 
          ON AUC.auction_status_id = STT.auction_status_id 
      WHERE has_winner = 1
        AND auction_status_name NOT IN ('Cancelled', 'Deleted') 
  ) AUC 
  WHERE auc_row = 1
)

, SalesChannel_Mod AS (
SELECT Cars.car_id 
  , CASE WHEN auction_buy_now_price = auction_start_price
      THEN 'YardsaleBuyNow' ELSE Cars.sales_channel 
    END AS sales_channel
  FROM Cars 
  LEFT JOIN LastWinnerAuction WIN 
    ON Cars.car_id = WIN.car_id
)

, CarAllowances_Grp AS (
SELECT car_id
  , SUM(allowance_sum) AS car_allowance 
  FROM [CarAllowances] 
  GROUP BY car_id
)

-- SUBTYPE_0, SUBTYPE_1, TAXLESS_2 se usan en la tabla final, pero también se usan en tablas posteriores. 
, CarsSubtype_0 AS (
SELECT car_id
  , source_car_id
  , car_selling_price_total
  , COALESCE(CASE WHEN client_subtype = '' THEN NULL ELSE client_subtype END,
        CASE WHEN purchase_channel = 'B2B' THEN 'business' ELSE NULL END) AS client_subtype
  FROM Cars
)

, CarsSources_1 AS (
SELECT C0.car_id
  , COALESCE(C0.client_subtype, C1.client_subtype, C2.client_subtype) AS client_subtype 
  , COALESCE(C1.car_selling_price_total, C2.car_selling_price_total) AS source_car_selling_price_total
  FROM CarsSubtype_0 C0
  LEFT JOIN CarsSubtype_0 C1 
    ON C0.source_car_id = C1.car_id 
  LEFT JOIN CarsSubtype_0 C2 
    ON C1.source_car_id = C2.car_id
)

, CarsTaxless_2 AS (
SELECT Cars.car_id
  , SUB.client_subtype
  , CASE WHEN SUB.[client_subtype] = 'person' THEN car_purchase_price_car 
      WHEN SUB.[client_subtype] IS NOT NULL THEN car_purchase_price_car/1.16 
      ELSE NULL END AS car_purchase_price_car_taxless
  , CASE WHEN SUB.[client_subtype] = 'person'
      THEN (car_selling_price_car +  -- MIN(car_purchase_price_car, car_selling_price_car)
          0.16*(CASE WHEN car_purchase_price_car <= car_selling_price_car
          THEN car_purchase_price_car ELSE car_selling_price_car END))/1.16
      WHEN SUB.[client_subtype] IS NOT NULL THEN car_selling_price_car/1.16
      ELSE NULL END AS car_selling_price_car_taxless
  FROM Cars
  LEFT JOIN CarsSources_1 SUB
    ON Cars.car_id = SUB.car_id
)

, CarsTaxless_3 AS (
  SELECT Cars.car_id
  , TXL.client_subtype
  , TXL.car_purchase_price_car_taxless
  , TXL.car_purchase_price_car_taxless AS car_purchase_taxless
  , TXL.car_purchase_price_car_taxless + Cars.car_purchase_price_total - Cars.car_purchase_price_car
        AS car_purchase_price_total_taxless
  , TXL.car_selling_price_car_taxless
  , TXL.car_selling_price_car_taxless AS car_selling_taxless
  , TXL.car_selling_price_car_taxless + Cars.car_selling_price_total - Cars.car_selling_price_car
        AS car_selling_price_total_taxless
  FROM Cars
  LEFT JOIN CarsTaxless_2 TXL 
    ON Cars.car_id = TXL.car_id 
)

-- Because Dates give us trouble. 
, CarsDates AS (
  SELECT Cars.car_id 
  , CAST(DATEADD(HOUR, 1, car_purchased_date)
      AS DATE) AS car_purchased_date
  , CAST(DATEADD(HOUR, 1, [car_handedover_from_seller])
      AS DATE) AS car_handedover_from_seller  
  , CAST(DATEADD(HOUR, 1,   -- Esta línea es importante------------
      COALESCE(auction_last_date, car_sold_date)) 
      AS DATE) AS reserved_at_date
  , reserved_at_date AS original_reserved_at_date
  , CAST(DATEADD(HOUR, 1, [car_handedover_to_buyer]) 
      AS DATE) AS car_handedover_to_buyer
  FROM Cars
  LEFT JOIN LastWinnerAuction
    ON LastWinnerAuction.car_id = Cars.car_id
)

, CarsCalculations AS (
  SELECT Cars.car_id 
  , CASE WHEN CDT.reserved_at_date IS NOT NULL 
      AND car_selling_status IN ('SOLD', 'RETURNED', 'CONSIGNED', 'CONFIRMED')
      THEN 1 ELSE 0 END AS sold_confirmed
  , CASE WHEN CDT.original_reserved_at_date IS NOT NULL 
        AND car_selling_status IN ('SOLD', 'RETURNED', 'CONSIGNED', 'CONFIRMED')
      THEN 1 ELSE 0 END AS original_sold_confirmed
  , CASE WHEN CDT.car_handedover_to_buyer IS NOT NULL 
      AND car_selling_status IN ('SOLD', 'RETURNED', 'CONSIGNED') 
      THEN 1 ELSE 0 END AS sold_delivered
  , CASE WHEN (purchased_by IS NOT NULL OR purchase_channel = 'B2B') 
      AND purchase_channel != 'ReturnedCar'
      AND car_legal_status IN ('US', 'BUYER')
      THEN 1 ELSE 0 END AS was_purchased
  , CASE WHEN (car_physical_status IN ('ATOURLOCATION', 'INTRANSIT')
      AND car_selling_status IN ('AVAILABLE', 'RESERVED', 'NOTAVAILABLE', 'PENDINGCLEARANCE', 'CONFIRMED', 'CONSIGNED')
      AND car_current_location NOT LIKE '%B2B%'
      AND car_current_location NOT LIKE '%Buyer%'
      AND car_current_location IS NOT NULL 
      AND purchase_channel IS NOT NULL
      AND purchase_channel != ''
      AND car_legal_status IS NOT NULL)
      OR (car_selling_status = 'INTERNALUSE') 
      THEN 1 ELSE 0 END AS inventory_isin
  , DATEDIFF(DAY, CDT.reserved_at_date, 
        COALESCE( CDT.car_handedover_to_buyer, GETDATE())) AS delivery_days
  , DATEDIFF(DAY, CDT.car_purchased_date, 
        COALESCE( CDT.reserved_at_date, GETDATE())) AS selling_days
  , DATEDIFF(DAY, CDT.car_handedover_from_seller, 
        COALESCE( CDT.car_handedover_to_buyer, GETDATE()) ) AS inventory_days
  -- , CASE WHEN (car_id IN (SELECT yardsale_car_id FROM Yardsales)) 
  --   THEN 1 ELSE 0 END AS is_yardsale
  , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
      car_purchase_location, 
          'IP -', 'IP '), 
          'IP  ' , 'IP '), 
          'IP ', ''), 
        '5 de Mayo', 'Toluca'), 
        'Americas Santa Tere', 'Américas Santa Tere'), 
        'Puebla', 'La Noria'), 
        'Ecatepec Gran Plaza', 'Ecatepec Grand Plaza'), 
        'Insurgentes Sur', 'Insurgentes'), 
        'Insurgentes', 'Insurgentes Sur'), 
        'Chedraui Ajusco', 'Ajusco'), 
        'Ajusco', 'Chedraui Ajusco'), 'Tonala', 'Tonalá'), 
        'Santa Monica', 'Santa Mónica'), 
        'Santa Fé', 'Santa Fe'), 
        'Leon', 'León'), 
        'Armas', 'Azcapotzalco'), 
        'Cuna del Futbol', 'Pachuca'), 
        'Aguilas', 'Las Águilas'), 
        'Anzures', 'Thiers'), 
        'Jinetes Arboledas', 'Arboledas'
    ) AS car_purchase_location
  , CASE WHEN CHARINDEX('-', initial_valuation_price) <> 0 
    THEN  0.5*(CAST(SUBSTRING([initial_valuation_price], 1, CHARINDEX('-',initial_valuation_price) - 1) AS float)) + 
          0.5*(CAST(SUBSTRING([initial_valuation_price], CHARINDEX('-',initial_valuation_price) + 1, 
          LEN(initial_valuation_price) - CHARINDEX('-',initial_valuation_price)) AS float))
    ELSE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(initial_valuation_price,
          ' ', ''),
          '$', ''),
          ',', ''),
          '.', ''),
          'O', '0'),
          '.00','')
    END AS initial_valuation_price 
  FROM Cars
  LEFT JOIN CarsDates CDT
    ON Cars.car_id = CDT.car_id
)

, CarsConsigned AS (
  SELECT car_id 
  , CAST(MAX(car_history_change_last_modified) AS DATE) AS consigned_date
  , COUNT(car_id) AS times_consigned
  FROM CarChangesHistory 
  WHERE car_history_change_value = 'CONSIGNED'
  GROUP BY car_id   
)

, SellingStatusLastChange AS (
  SELECT car_id
  , MAX(car_history_change_created) car_selling_status_last_modified
  FROM CarChangesHistory 
  WHERE car_history_change_type = 'status' 
    AND car_history_change_value IN (SELECT DISTINCT car_selling_status FROM Cars)
  GROUP BY car_id
)

, LastChange AS (
  SELECT car_id 
  , Max(car_history_change_last_modified) AS updated_at
  FROM CarChangesHistory
  GROUP BY car_id
)

SELECT Cars.[car_id]
--LIMPIEZA DE INITIAL VALUATION
, 'MX-' + CAST([internal_car_id] AS VARCHAR) as internal_car_id
, CDT.car_purchased_date
, [car_manufacturer_name]
, [car_model_name]
, [year_manufactured]
, [car_manufacturer_name] + ' - ' + [car_model_name] + ' - ' 
    + cast(year_manufactured as varchar) as car_name
-- , [car_last_modified]
, CDT.car_handedover_from_seller  
, CDT.car_handedover_to_buyer
-- , [available_for_pickup]
, CDT.reserved_at_date
, CDT.original_reserved_at_date
, [car_created]
, [car_trim]
, [car_fuel]
, [car_mileage]
, [car_transmission]
, [car_color]
, [car_vin]
, [car_license_plate]
, [car_engine_number]
-- , [car_registered_city]
-- , [car_technical_check_expiration]
, [car_selling_status]
, [car_physical_status]
, [car_legal_status]
, [car_purchase_payment_status]
, [car_selling_payment_status]
, [car_document_status]
-- , [car_type]
, [car_current_location]
, CALC.car_purchase_location 
, [car_selling_location]
, [car_selling_status_last_modified]
, [number_of_transits]
, [car_purchase_price_car]   --, [car_purchase_price_car_usd]

, Cars.car_purchase_price_total - Cars.car_purchase_price_car AS [car_purchase_price_other] --, [car_purchase_price_other_usd]
, Cars.car_purchase_price_total - Cars.car_purchase_price_car AS otros_costos
, [car_purchase_price_total] --, [car_purchase_price_total_usd]
, TXL .car_purchase_taxless
, TXL.car_purchase_price_car_taxless
, TXL.car_purchase_price_total_taxless 
, TXL.car_selling_taxless
, TXL.car_selling_price_car_taxless
, TXL.car_selling_price_total_taxless
, [car_selling_price_car]
, [car_selling_price_total]
, [source_car_selling_price_total]
-- , [seller_expected_price]    --, [seller_expected_price_usd] 
--, [first_offer_price]        --, [first_offer_price_usd]
--, [final_offer_price]        --, [final_offer_price_usd]
-- , [competitor_price]         --, [competitor_price_usd]
, [purchase_channel]
, [purchased_by]
, CHNL.[sales_channel]
, DLR.dealer_manager_name AS sold_by
, sold_to_id
, DLR.[dealer_name]
, DLR.dealer_tax_id
, DLR.dealer_company_name
, [sold_to]
, [invoice_date]
--, [car_selling_price_car_usd]
/* Intermediate Prices
-- PURCHASE
, [car_purchase_price_transport]--, [car_purchase_price_transport_usd]
, [car_purchase_price_delivery]--, [car_purchase_price_delivery_usd]
, [car_purchase_price_repairs]--, [car_purchase_price_repairs_usd]
, [car_purchase_price_documentation]--, [car_purchase_price_documentation_usd]
, [car_purchase_price_delivery_driver]--, [car_purchase_price_delivery_driver_usd]
, [car_purchase_price_penalty]--, [car_purchase_price_penalty_usd]
, [car_purchase_price_debts]--, [car_purchase_price_debts_usd]
, [car_purchase_price_penalty_projected]--, [car_purchase_price_penalty_projected_usd]
, [car_purchase_price_logistics]--, [car_purchase_price_logistics_usd]
-- SELLING
, [car_selling_price_iaas]--, [car_selling_price_iaas_usd]
, [car_selling_price_valuations]--, [car_selling_price_valuations_usd]
, [car_selling_price_penalty]--, [car_selling_price_penalty_usd]
, [car_selling_price_interest_bank]--, [car_selling_price_interest_bank_usd]
, [car_selling_price_interest]--, [car_selling_price_interest_usd]
, [car_selling_price_discount_fleet]--, [car_selling_price_discount_fleet_usd]
, [car_selling_price_total_usd]
*/
, [car_outgoing_payment_total]     --, [car_outgoing_payment_total_usd]
, [car_outgoing_payment_total_net] --, [car_outgoing_payment_total_net_usd]
, [car_incoming_payment_total]     --, [car_incoming_payment_total_usd]
, [car_incoming_payment_total_net] --, [car_incoming_payment_total_net_usd]
, [car_sold_date]
, [latest_outgoing_payment_date]
, [latest_incoming_payment_date]
, Cars.source_car_id
-- , [visible_in_auctions_count]
-- , [paid_for]
-- , [received_for]
--, [country_id]
--, [area_id]
-- DE OTRAS TABLAS. 
, TXL.client_subtype AS client_subtype
, CALC.sold_delivered 
, CALC.original_sold_confirmed
, CALC.sold_confirmed
, CALC.was_purchased
--, CALC.selling_days
, CALC.inventory_days
, CALC.inventory_isin
, CALC.initial_valuation_price
, AUC.auction_class
, AUC.auction_last_date
, ALW.car_allowance
, b2b_deal
, Cars.deleted_at
, QCS.inspection_qc_score
, COALESCE(QCS.inspector, 'Desconocido') AS inspector
, COALESCE(CHNG.updated_at, GETDATE()) AS updated_at
FROM Cars
LEFT JOIN [CarManufacturers] CMNF 
  ON Cars.car_manufacturer_id = CMNF.car_manufacturer_id
LEFT JOIN [CarModel] CMDL 
  ON Cars.car_model_id = CMDL.car_model_id
LEFT JOIN [Dealers] DLR 
  ON Cars.sold_to_id = DLR.dealer_id
LEFT JOIN CarsDates CDT 
  ON Cars.car_id = CDT.car_id
LEFT JOIN CarsSources_1 SUB
  ON Cars.car_id = SUB.car_id
LEFT JOIN CarsTaxless_3 TXL
  ON Cars.car_id = TXL.car_id
LEFT JOIN CarsCalculations CALC
  ON Cars.car_id = CALC.car_id
LEFT JOIN CarAllowances_Grp ALW
  ON Cars.car_id = ALW.car_id
LEFT JOIN LastWinnerAuction AUC 
  ON Cars.car_id = AUC.car_id
LEFT JOIN SalesChannel_Mod CHNL 
  ON Cars.car_id = CHNL.car_id
LEFT JOIN CarsConsigned CNSG
  ON Cars.car_id = CNSG.car_id
LEFT JOIN SellingStatusLastChange STTCHN
  ON Cars.car_id = STTCHN.car_id
LEFT JOIN LastChange CHNG
  ON Cars.car_id = CHNG.car_id
LEFT JOIN QCScore QCS
  ON Cars.car_id = QCS.car_id
WHERE Cars.deleted_at IS NULL 
  AND CHNG.updated_at >= :from_when 