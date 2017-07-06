# OPV_Dags

One task by executor to two => 1 Minutes 43 secondes to 2 minutes 24 secondes

Pour traiter 191 panoramas avec un process par worker
temps_total = 191 * 1.73 = 330.43 => 5h30 pour un worker
2h45 sur deux workers

Pour traiter 191 panoramas avec deux process par worker:
temps_total = 191 * 2.41 = 460.31 / 2 (car deux process par worker) =  230.15500000000000000000 => 3h49
1h54 sur deux worker