# F3: bronce (exactamente una vez) y los 54 duplicados del puente de la S08

Lectura de `data/local/bronce/` con pyarrow, el 2026-10-29 (después del reinicio y de las pruebas).

## Cada mensaje de Kafka exactamente una vez

```text
filas: 7107 | pares (partición, offset) únicos: 7107 | repetidos: 0
partición 0: offsets 0..6790 -> 6791 filas, huecos 0
partición 1: offsets 0..236 -> 237 filas, huecos 0
partición 2: offsets 0..78 -> 79 filas, huecos 0
```

## Eventos de prueba en bronce (bronce no aplica watermark ni deduplicación)

```text
{'eventId': 'prueba-f3-tardio-f0c5fc05', 'sensorId': 'esp32-prueba-f3', 'kafka_particion': 1, 'kafka_offset': 230, 'origen': 'esp32-fisico', 'timestamp': 1793237281348}
{'eventId': 'prueba-f3-duplicado-7c6dedcc', 'sensorId': 'esp32-prueba-f3', 'kafka_particion': 1, 'kafka_offset': 231, 'origen': 'esp32-fisico', 'timestamp': 1793244528859}
{'eventId': 'prueba-f3-duplicado-7c6dedcc', 'sensorId': 'esp32-prueba-f3', 'kafka_particion': 1, 'kafka_offset': 232, 'origen': 'esp32-fisico', 'timestamp': 1793244528859}
```

## Hallazgo: 54 lecturas reales publicadas dos veces por el puente de la S08

`dropDuplicatesWithinWatermark` descartó 54 filas en el primer lote de `rapidas_agg` y `esp32_detalle`. Son 54 `eventId` del ESP32 con dos copias cada uno, todas en la partición 0 entre los offsets 1620 y 1727: mensajes del puente de la S08 (`lambda26-aqua-bridge`, offsets 0-2648), que reenvió lecturas con `--replay`. El puente nuevo (F2) no produjo duplicados (auditoría de F2: 0 `eventId` repetidos).

```text
offset   eventId                 timestamp
1620     -P4aE4jyZqP05LA98vPw    1792716593794
1621     -P4aE5y4QMC1pohSKiJJ    1792716598794
1622     -P4aE7Izn5gtamsdyOpo    1792716603794
1623     -P4aE8PZ1EJy6OliaD7Q    1792716608794
1624     -P4aE9cO2JblDV3Dl1_0    1792716613794
1625     -P4aEAq_Xp2pGY40pysr    1792716618793
1626     -P4aEC3eKmuLYqbt54h-    1792716623793
1627     -P4aEDHl4bGMfqD1IkQf    1792716628793
1628     -P4aEEVtZfOm-uOMAAqf    1792716633793
1629     -P4aEFj2pG9d1KoQuhcC    1792716638793
1630     -P4aEGxBL-iP6ijW6X8w    1792716643793
1631     -P4aEIAMChnx7SBFHQKg    1792716648794
1632     -P4aEJOTut7TM9LbtjKN    1792716653793
1633     -P4aEKbWAmi1ehkNQaCT    1792716658793
1634     -P4aELpjUVNRtT7BTQUs    1792716663793
1635     -P4aEN2jzlDA6yiI-gpw    1792716668793
1636     -P4aEOHD-yKH4jfLTp7Q    1792716673793
1637     -P4aEPV-Jc6l0enjGRo2    1792716678793
1638     -P4aEQi8Lv8T_Eh2LZm8    1792716683793
1639     -P4aERwGTglbPIgYfOTl    1792716688793
1640     -P4aET9Rmsz5lEeYQW3X    1792716693793
1641     -P4aEUNXnxw3VxjWgz-B    1792716698793
1642     -P4aEVaeydrldTIV_O4h    1792716703793
1643     -P4aEWox0Uv14kEbqrrr    1792716708793
1644     -P4aEYBdwPV-4Lq6U0IH    1792716713793
1645     -P4aEZGfShQu4Uf9UWxc    1792716718793
1646     -P4aE_U9NT61aYvevxdG    1792716723793
1647     -P4aEahGmDFNf1xIgbtQ    1792716728793
1648     -P4aEbvOOd8tudNBjMR0    1792716733793
1649     -P4aEd8j7RhhpnMtECyE    1792716738793
1650     -P4aEeMdZeiXv2QXUDlk    1792716743793
1651     -P4aEf_k9nJCxyeW4Cjb    1792716748793
1652     -P4aEgnttMPiiPUpmLCP    1792716753793
1653     -P4aEi13kQ9TmOUTU0fb    1792716758793
1654     -P4aEjF6ZbUFE-RhIYYs    1792716763793
1655     -P4aEkTGiavxxjNHHdkG    1792716768793
1656     -P4aElgToPc4UIsN6p1g    1792716773793
1657     -P4aEmu_0AgHNoXSjX0a    1792716778793
1658     -P4aEo7eXcTRLBCbGh-S    1792716783793
1659     -P4aEpM6nuYMuGBDrSO7    1792716788793
1660     -P4aEqZsNVcd65WCXd7K    1792716793793
1661     -P4aErn3pIh51ch_bAU7    1792716798793
1662     -P4aEt024nV_23HFv_GD    1792716803768
1663     -P4aEuDutfkdu1CRcTBE    1792716808766
1664     -P4aEvS2Goz5dsg2rnum    1792716813766
1665     -P4aEwf6ECYI7goRajuh    1792716818766
1666     -P4aExtEnYZ4tN_5mjyT    1792716823766
1667     -P4aEz6S9jza6T8I0__n    1792716828766
1668     -P4aF-KSVHDk7E58v_Pm    1792716833766
1669     -P4aF0YvhcK7B5mOgnXa    1792716838766
1670     -P4aF1lhOOl1c85R60WM    1792716843766
1671     -P4aF2zvc2Scv1BK60ih    1792716848766
1672     -P4aF4D-_NBa3QBZxsS0    1792716853766
1673     -P4aF5R6DJ8sRo5yD4NF    1792716858766
1674     -P4aE4jyZqP05LA98vPw    1792716593794
1675     -P4aE5y4QMC1pohSKiJJ    1792716598794
1676     -P4aE7Izn5gtamsdyOpo    1792716603794
1677     -P4aE8PZ1EJy6OliaD7Q    1792716608794
1678     -P4aE9cO2JblDV3Dl1_0    1792716613794
1679     -P4aEAq_Xp2pGY40pysr    1792716618793
1680     -P4aEC3eKmuLYqbt54h-    1792716623793
1681     -P4aEDHl4bGMfqD1IkQf    1792716628793
1682     -P4aEEVtZfOm-uOMAAqf    1792716633793
1683     -P4aEFj2pG9d1KoQuhcC    1792716638793
1684     -P4aEGxBL-iP6ijW6X8w    1792716643793
1685     -P4aEIAMChnx7SBFHQKg    1792716648794
1686     -P4aEJOTut7TM9LbtjKN    1792716653793
1687     -P4aEKbWAmi1ehkNQaCT    1792716658793
1688     -P4aELpjUVNRtT7BTQUs    1792716663793
1689     -P4aEN2jzlDA6yiI-gpw    1792716668793
1690     -P4aEOHD-yKH4jfLTp7Q    1792716673793
1691     -P4aEPV-Jc6l0enjGRo2    1792716678793
1692     -P4aEQi8Lv8T_Eh2LZm8    1792716683793
1693     -P4aERwGTglbPIgYfOTl    1792716688793
1694     -P4aET9Rmsz5lEeYQW3X    1792716693793
1695     -P4aEUNXnxw3VxjWgz-B    1792716698793
1696     -P4aEVaeydrldTIV_O4h    1792716703793
1697     -P4aEWox0Uv14kEbqrrr    1792716708793
1698     -P4aEYBdwPV-4Lq6U0IH    1792716713793
1699     -P4aEZGfShQu4Uf9UWxc    1792716718793
1700     -P4aE_U9NT61aYvevxdG    1792716723793
1701     -P4aEahGmDFNf1xIgbtQ    1792716728793
1702     -P4aEbvOOd8tudNBjMR0    1792716733793
1703     -P4aEd8j7RhhpnMtECyE    1792716738793
1704     -P4aEeMdZeiXv2QXUDlk    1792716743793
1705     -P4aEf_k9nJCxyeW4Cjb    1792716748793
1706     -P4aEgnttMPiiPUpmLCP    1792716753793
1707     -P4aEi13kQ9TmOUTU0fb    1792716758793
1708     -P4aEjF6ZbUFE-RhIYYs    1792716763793
1709     -P4aEkTGiavxxjNHHdkG    1792716768793
1710     -P4aElgToPc4UIsN6p1g    1792716773793
1711     -P4aEmu_0AgHNoXSjX0a    1792716778793
1712     -P4aEo7eXcTRLBCbGh-S    1792716783793
1713     -P4aEpM6nuYMuGBDrSO7    1792716788793
1714     -P4aEqZsNVcd65WCXd7K    1792716793793
1715     -P4aErn3pIh51ch_bAU7    1792716798793
1716     -P4aEt024nV_23HFv_GD    1792716803768
1717     -P4aEuDutfkdu1CRcTBE    1792716808766
1718     -P4aEvS2Goz5dsg2rnum    1792716813766
1719     -P4aEwf6ECYI7goRajuh    1792716818766
1720     -P4aExtEnYZ4tN_5mjyT    1792716823766
1721     -P4aEz6S9jza6T8I0__n    1792716828766
1722     -P4aF-KSVHDk7E58v_Pm    1792716833766
1723     -P4aF0YvhcK7B5mOgnXa    1792716838766
1724     -P4aF1lhOOl1c85R60WM    1792716843766
1725     -P4aF2zvc2Scv1BK60ih    1792716848766
1726     -P4aF4D-_NBa3QBZxsS0    1792716853766
1727     -P4aF5R6DJ8sRo5yD4NF    1792716858766
```
