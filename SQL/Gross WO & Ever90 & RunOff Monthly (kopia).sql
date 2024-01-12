IF OBJECT_ID('tempdb..#frozen') is not null
DROP TABLE #frozen;
with frozen as (
select AccountNumber as 'loanaccountnumber'
,MIN(case when CollectionDate<>'' then CollectionDate else null end) as CollectionDate
,MIN(case when FrozenDate <>'' then FrozenDate else null end) as FrozenDate
,MIN(case when WODate<>'' then WOdate else null end) as WODate
,MIN(case when DefaultDate<>'' then DefaultDate else null end) as DefaultDate
from [reporting-db].[nystart].[LoanPortfolio]
where 1=1
--and AccountNumber='7839954'
group by AccountNumber
),

frozen2 as (
select * 
,case when FrozenDate<='2018-01-01' then FrozenDate
when DefaultDate >='2018-01-01'then DefaultDate
when CollectionDate >='2018-01-01'then CollectionDate
when WODate >='2022-12-01'then WODate
else '' end as 'SendToCollection'

from frozen ),


MOB_PLUS_90 AS (
SELECT DISTINCT 
    m.[AccountNumber],
    m.mob,
    m.DisbursedDate,
    m.SnapshotDate,
    m.CurrentAmount as Amount,
    
    -- Lag this one to get the amount that was still left at time of collection 
    --LAG(m.CurrentAmount) OVER (PARTITION BY m.AccountNumber ORDER BY m.mob) as L1_Amount,


  
    m.Ever90,
    m.CurrentAmount,
    m.IsOpen,

    f.SendToCollection,


    
CASE 
    WHEN SendToCollection = '1900-01-01' THEN 0
    WHEN Convert(date, f.SendToCollection) <= Convert(date, m.SnapshotDate) THEN 1
    ELSE 0 
END AS WO_flag_ever

,CASE 
    WHEN SendToCollection = '1900-01-01' THEN 0
    WHEN YEAR(Convert(date, f.SendToCollection)) = YEAR(Convert(date, m.SnapshotDate))
         AND MONTH(Convert(date, f.SendToCollection)) = MONTH(Convert(date, m.SnapshotDate)) THEN 1
    ELSE 0 

END AS WO_flag , 


-- Controll Modhi numbers
ff.RemainingCapital,
ff.PurchasePrice

FROM 
    [Reporting-db].[nystart].[LoanPortfolioMonthly] AS m
LEFT JOIN 
    frozen2 AS f ON m.AccountNumber = f.loanaccountnumber

left join [Reporting-db].[nystart].[ForwardFlow] as fF on m.Accountnumber = ff.AccountNumber

    WHERE IsMonthEnd =1 and DisbursedDate >= '2015-01-01' ),


Initall_Amount as (  Select max(CurrentAmount) as maxAmount , AccountNumber from MOB_PLUS_90 GROUP BY AccountNumber),



Collection_accounts as (  Select distinct  AccountNumber, CurrentAmount as WO_Amount , mob  from MOB_PLUS_90  where WO_flag_ever = 1 GROUP BY AccountNumber, mob, CurrentAmount), 


COLL_MIN_Month as (SELECT AccountNumber , min(mob) as collMob from Collection_accounts GROUP by AccountNumber) ,


Collection_accounts_complete as (SELECT     ca.*    

from COLL_MIN_Month as cm 
inner join Collection_accounts as ca    on cm.AccountNumber = ca.AccountNumber and cm.collMob = ca.mob ),




-- THIS ONE IS DONE TO ONLY TAKE THE RUN OF FROM ACTIVE ACCOUNTS SINCE THAT IS WHAT IS ABLE TO GET BACK PIT WHEN MULTIPLYING GWO % WITH THE EAD
MOB_PLUS_90_ADJUSTED as (
    
    
SELECT 
    m.*,

    i.maxAmount,


    CASE 
        WHEN i.maxAmount > 0 AND CAST(c.AccountNumber AS INT) > 0 THEN 0 
        ELSE i.maxAmount 
    END AS MaxAmountAdjusted, -- Renamed for clarity
    CASE 
        WHEN m.CurrentAmount > 0 AND CAST(c.AccountNumber AS INT) > 0 THEN 0 
        ELSE m.CurrentAmount 
    END AS CurrentAmountAdjusted -- Renamed for clarity

    -- 

    ,c.WO_Amount





FROM MOB_PLUS_90 AS m 

    LEFT JOIN Collection_accounts_complete AS c ON m.AccountNumber = c.AccountNumber
    
    INNER JOIN Initall_Amount AS i ON m.AccountNumber = i.AccountNumber

),


RUN_OFF as ( SELECT 
        *, 

        CASE 
            WHEN MaxAmountAdjusted IS NULL OR MaxAmountAdjusted = 0 THEN 0 
            ELSE CurrentAmountAdjusted / NULLIF(MaxAmountAdjusted, 0) 
        END AS run_off

    FROM MOB_PLUS_90_ADJUSTED  )


SELECT * ,

    -- Modhi Accounts Ever
    CASE 
        WHEN WO_flag_ever = 1 and RemainingCapital > 0 THEN RemainingCapital 
        WHEN WO_flag_ever = 1 and RemainingCapital is null THEN WO_Amount 
        ELSE 0 
    END as WO_Amount2



    ,CASE 
        WHEN WO_flag = 1  and RemainingCapital > 0 THEN RemainingCapital 
        WHEN WO_flag = 1  and RemainingCapital is null THEN CurrentAmount 
        ELSE 0 
    END as WO_Amount_PiT


 from RUN_OFF 

 --where 
 
 --SnapshotDate > '2023-01-01' --and WO_flag_ever = 1
 
 --and  AccountNumber = '7903487'

 order by AccountNumber ,mob

