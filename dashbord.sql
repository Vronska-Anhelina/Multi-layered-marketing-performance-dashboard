WITH acc_metric AS( --metrics for accounts (basic)
SELECT date,
country,
send_interval,
is_verified,
is_unsubscribed,
COUNT(DISTINCT account_id) AS account_cnt
FROM `DA.account` acc
JOIN `DA.account_session` acs
ON acc.id=acs.account_id
JOIN `DA.session` ss
ON ss.ga_session_id=acs.ga_session_id
JOIN `DA.session_params` sp
ON acs.ga_session_id=sp.ga_session_id
GROUP BY date,country,send_interval,is_verified,is_unsubscribed ),
email_metric AS( --email metrics (basic)
  SELECT DATE_ADD(ss.date,INTERVAL es.sent_date DAY) AS date
  ,country,
  send_interval,
  is_verified,
  is_unsubscribed,
  COUNT(DISTINCT es.id_message) AS sent_msg,
  COUNT(DISTINCT eo.id_message) AS open_msg,
  COUNT(DISTINCT ev.id_message) AS visit_msg
  FROM `DA.email_sent` es
  LEFT JOIN `DA.email_open` eo
  ON es.id_message=eo.id_message
  LEFT JOIN `DA.email_visit` ev    
  ON es.id_message=ev.id_message
  JOIN `DA.account` acc
  ON es.id_account=acc.id
  JOIN `DA.account_session` acs
  ON acc.id=acs.account_id
  JOIN `DA.session` ss
  ON acs.ga_session_id=ss.ga_session_id
  JOIN `DA.session_params` sp
  ON acs.ga_session_id=sp.ga_session_id
  GROUP BY date,country,send_interval,is_verified,is_unsubscribed),
unions AS( --join tables
     SELECT date,
     country,
     send_interval,
     is_verified,
     is_unsubscribed
     ,account_cnt,0 AS sent_msg,0 AS open_msg, 0 AS visit_msg
    FROM acc_metric




    UNION ALL
    SELECT date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    0 AS account_cnt,
    sent_msg,open_msg, visit_msg
     FROM email_metric),
agg AS ( -- avoid data duplication
   SELECT date,country,is_verified,is_unsubscribed,SUM(account_cnt) AS account_cnt,
   SUM(sent_msg) AS sent_msg,
      SUM(open_msg) as open_msg,
   sum(visit_msg) as visit_msg
      from unions
     group by date,country,is_verified,is_unsubscribed),
 add_metric AS (  -- calculation of additional metrics through window functions
      SELECT *,
       SUM(account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
      SUM(sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt
      FROM agg),
ranks AS ( --ranks
        SELECT *,
         DENSE_RANK() OVER(ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
        DENSE_RANK() OVER (ORDER BY  total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
        FROM add_metric)
SELECT  * --final output
        FROM ranks
    WHERE rank_total_country_sent_cnt<=10 OR rank_total_country_account_cnt<=10;
