#include "mainclass.h"

MainClass::MainClass(QString configFileName, QObject *parent) : QObject(parent)
{
    activemq::library::ActiveMQCPP::initializeLibrary();

    cfg = readCFG(configFileName);

    qRegisterMetaType<_EEWINFO>("_EEWINFO");
    qRegisterMetaType< QMultiMap<int, _QSCD_FOR_MULTIMAP> >("QMultiMap<int,_QSCD_FOR_MULTIMAP>");

    initProj();

    qscdDB = QSqlDatabase::addDatabase("QMYSQL");
    qscdDB.setHostName(cfg.db_ip);
    qscdDB.setDatabaseName(cfg.db_name);
    qscdDB.setUserName(cfg.db_user);
    qscdDB.setPassword(cfg.db_passwd);
    qscdDB.setConnectOptions("MYSQL_OPT_RECONNECT=1");

    eventModel = new QSqlQueryModel();
    networkModel = new QSqlQueryModel();
    affiliationModel = new QSqlQueryModel();
    siteModel = new QSqlQueryModel();

    staList.clear();
    QSCD20_DATA_HOUSE.clear();
    QSCD100_DATA_HOUSE.clear();

    readStationListfromDB();

    writeLog(cfg.logDir, cfg.processName, "======================================================");

    if(cfg.eew_amq_topic != "")
    {
        QString eewFailover = "failover:(tcp://" + cfg.eew_amq_ip + ":" + cfg.eew_amq_port + ")";

        rvEEW_Thread = new RecvEEWMessage;
        if(!rvEEW_Thread->isRunning())
        {
            rvEEW_Thread->setup(eewFailover, cfg.eew_amq_user, cfg.eew_amq_passwd, cfg.eew_amq_topic, true, false);
            connect(rvEEW_Thread, SIGNAL(_rvEEWInfo(_EEWINFO)), this, SLOT(rvEEWInfo(_EEWINFO)));
            rvEEW_Thread->start();
        }
    }

    if(cfg.qscd20_kiss_amq_topic != "")
    {
        QString failover = "failover:(tcp://" + cfg.qscd20_kiss_amq_ip + ":" + cfg.qscd20_kiss_amq_port + ")";

        rvQSCD20_KISS_Thread = new RecvQSCD20Message;
        if(!rvQSCD20_KISS_Thread->isRunning())
        {
            rvQSCD20_KISS_Thread->setup(failover, cfg.qscd20_kiss_amq_user, cfg.qscd20_kiss_amq_passwd,
                                         cfg.qscd20_kiss_amq_topic, true, false, "KISS", "ALL");
            connect(rvQSCD20_KISS_Thread, SIGNAL(sendQSCDtoMain(QMultiMap<int, _QSCD_FOR_MULTIMAP>)),
                    this, SLOT(extractQSCD20(QMultiMap<int, _QSCD_FOR_MULTIMAP>)));
            rvQSCD20_KISS_Thread->start();
        }
    }

    if(cfg.qscd20_mpss_amq_topic != "")
    {
        QString failover = "failover:(tcp://" + cfg.qscd20_mpss_amq_ip + ":" + cfg.qscd20_mpss_amq_port + ")";

        rvQSCD20_MPSS_Thread = new RecvQSCD20Message;
        if(!rvQSCD20_MPSS_Thread->isRunning())
        {
            rvQSCD20_MPSS_Thread->setup(failover, cfg.qscd20_mpss_amq_user, cfg.qscd20_mpss_amq_passwd,
                                         cfg.qscd20_mpss_amq_topic, true, false, "MPSS", "ALL");
            connect(rvQSCD20_MPSS_Thread, SIGNAL(sendQSCDtoMain(QMultiMap<int, _QSCD_FOR_MULTIMAP>)),
                    this, SLOT(extractQSCD20(QMultiMap<int, _QSCD_FOR_MULTIMAP>)));
            rvQSCD20_MPSS_Thread->start();
        }
    }

    if(cfg.qscd100_amq_topic != "")
    {
        QString failover = "failover:(tcp://" + cfg.qscd100_amq_ip + ":" + cfg.qscd100_amq_port + ")";

        rvQSCD100_Thread = new RecvQSCD100Message;
        if(!rvQSCD100_Thread->isRunning())
        {
            rvQSCD100_Thread->setup(failover, cfg.qscd100_amq_user, cfg.qscd100_amq_passwd,
                                         cfg.qscd100_amq_topic, true, false);
            connect(rvQSCD100_Thread, SIGNAL(sendQSCDtoMain(QMultiMap<int, _QSCD_FOR_MULTIMAP>)),
                    this, SLOT(extractQSCD100(QMultiMap<int, _QSCD_FOR_MULTIMAP>)));
            rvQSCD100_Thread->start();
        }
    }

    writeLog(cfg.logDir, cfg.processName, "EEWCORE Started");

    QTimer *systemTimer = new QTimer;
    connect(systemTimer, SIGNAL(timeout()), this, SLOT(doRepeatWork()));
    systemTimer->start(1000);
}

void MainClass::rvEEWInfo(_EEWINFO eewInfo)
{
    if(eewInfo.message_category == TEST)
        return;

    QString loc = getLocation(QString::number(eewInfo.latitude, 'f', 4), QString::number(eewInfo.longitude, 'f', 4));

    //strcpy(eewInfo.location, loc.toLatin1().constData());
    ll2xy4Large(pj_longlat, pj_eqc, eewInfo.longitude, eewInfo.latitude, &eewInfo.lmapX, &eewInfo.lmapY);
    ll2xy4Small(pj_longlat, pj_eqc, eewInfo.longitude, eewInfo.latitude, &eewInfo.smapX, &eewInfo.smapY);

    QDateTime et; et.setTimeSpec(Qt::UTC); et.setTime_t(eewInfo.origintime);

    if(eewInfo.message_type == NEW)
    {
        eventtime = eewInfo.origintime;
        dataFilePath = cfg.eventDir + "/" + et.toString("yyyy/MM") + "/" + QString::number(eewInfo.eew_evid);
    }

    QString message_category, message_type;
    if(eewInfo.message_category == LIVE) message_category = "LIVE";
    else if(eewInfo.message_category == TEST) message_category = "TEST";

    if(eewInfo.message_type == NEW) message_type = "NEW";
    else if(eewInfo.message_type == UPDATE) message_type = "UPDATE";
    else if(eewInfo.message_type == DELETE) message_type = "DELETE";

    QDateTime lddate = QDateTime::currentDateTimeUtc();
    lddate = convertKST(lddate);

    QString query = "INSERT INTO EEWINFO "
            "(eew_evid, version, message_category, nudmessagetype, magnitude, "
            "latitude, longitude, depth, "
            "origin_time, number_stations, "
            "lmapx, lmapy, smapx, smapy, location, lddate) values ("
            + QString::number(eewInfo.eew_evid) + ", " + QString::number(eewInfo.version) + ", '"
            + message_category + "', '" + message_type + "', " + QString::number(eewInfo.magnitude, 'f', 6) + ", "
            + QString::number(eewInfo.latitude, 'f', 6) + ", "
            + QString::number(eewInfo.longitude, 'f', 6) + ", "
            + QString::number(eewInfo.depth, 'f', 6) + ", "
            + QString::number(eewInfo.origintime) + ", "
            + QString::number(eewInfo.number_stations) + ", "
            + QString::number(eewInfo.lmapX) + ", " + QString::number(eewInfo.lmapY) + ", "
            + QString::number(eewInfo.smapX) + ", " + QString::number(eewInfo.smapY) + ", '"
            + loc + "', '" + lddate.toString("yyyy-MM-dd") + "')";

    runSQL(query, "EEWINFO");

    QDir evtFilePathD(dataFilePath + "/SRC");
    if(!evtFilePathD.exists())
        evtFilePathD.mkpath(".");

    // header file
    QFile evtHeaderFile(dataFilePath + "/SRC/eewInfo.txt");
    evtHeaderFile.open(QIODevice::WriteOnly | QIODevice::Append);
    QTextStream headerout(&evtHeaderFile);
    headerout << "EEWINFO=" << QString::number(eewInfo.eew_evid) << ":" << QString::number(eewInfo.version) << ":" <<
            message_category << ":" << message_type << ":" << QString::number(eewInfo.magnitude, 'f', 6) << ":" <<
            QString::number(eewInfo.latitude, 'f', 6) << ":" <<
            QString::number(eewInfo.longitude, 'f', 6) << ":" <<
            QString::number(eewInfo.depth, 'f', 6) << ":" <<
            QString::number(eewInfo.origintime) << ":" <<
            QString::number(eewInfo.number_stations) << ":" <<
            QString::number(eewInfo.lmapX) << ":" << QString::number(eewInfo.lmapY) << ":" <<
            QString::number(eewInfo.smapX) << ":" << QString::number(eewInfo.smapY) << ":" <<
            loc << ":" << lddate.toString("yyyy-MM-dd") << "\n";
    evtHeaderFile.close();

    // station file
    QFile staFile(dataFilePath + "/SRC/staInfo.txt");
    if(!staFile.exists())
    {
        staFile.open(QIODevice::WriteOnly | QIODevice::Append);
        QTextStream staout(&staFile);

        for(int i=0;i<staList.size();i++)
        {
            _STATION sta = staList.at(i);
            staout << sta.netSta << ":" << QString::number(sta.lat, 'f', 4) << ":"
                << QString::number(sta.lon, 'f', 4) << ":" << QString::number(sta.inUse) << "\n";
        }
        staFile.close();
    }
}

void MainClass::runSQL(QString query, QString tableName)
{
    openDB();
    if(tableName.startsWith("EEWINFO"))
    {
        eventModel->setQuery(query);
        if(eventModel->lastError().isValid())
        {
            qDebug() << eventModel->lastError();
            writeLog(cfg.logDir, cfg.processName, eventModel->lastError().text());
            writeLog(cfg.logDir, cfg.processName, "SQL ERROR\n" + query);

            return;
        }
        writeLog(cfg.logDir, cfg.processName, "SQL COMPLETE\n" + query);
    }
    qscdDB.close();
}

QString MainClass::getLocation(QString latS, QString lonS)
{
    QProcess process;
    QString cmd = find_loc_program + " " + latS + " " + lonS;
    process.start(cmd);
    process.waitForFinished(-1); // will wait forever until finished
    QString stdout = process.readAllStandardOutput();
    int leng = stdout.length();
    return stdout.left(leng-1);
}

void MainClass::initProj()
{
    if (!(pj_longlat = pj_init_plus("+proj=longlat +ellps=WGS84")))
    {
        qDebug() << "Can't initialize projection.";
        exit(1);
    }
    if (!(pj_eqc = pj_init_plus("+proj=eqc +ellps=WGS84")))
    {
        qDebug() << "Can't initialize projection.";
        exit(1);
    }
}

void MainClass::extractQSCD20(QMultiMap<int, _QSCD_FOR_MULTIMAP> mmFromAMQ)
{
    QMapIterator<int, _QSCD_FOR_MULTIMAP> i(mmFromAMQ);

    mutex.lock();
    while(i.hasNext())
    {
        i.next();
        QSCD20_DATA_HOUSE.insert(i.key(), i.value());
    }
    mutex.unlock();
}

void MainClass::extractQSCD100(QMultiMap<int, _QSCD_FOR_MULTIMAP> mmFromAMQ)
{
    QMapIterator<int, _QSCD_FOR_MULTIMAP> i(mmFromAMQ);

    mutex.lock();
    while(i.hasNext())
    {
        i.next();
        QSCD100_DATA_HOUSE.insert(i.key(), i.value());
    }
    mutex.unlock();
}

void MainClass::savePGAtoFile(int dt, QList<_QSCD_FOR_MULTIMAP> qscd20List, QList<_QSCD_FOR_MULTIMAP> qscd100List)
{
    QFile qscd20BinFile(dataFilePath + "/SRC/PGA20.dat");
    qscd20BinFile.open(QIODevice::WriteOnly | QIODevice::Append);
    QDataStream stream(&qscd20BinFile);

    for(int i=0;i<qscd20List.size();i++)
    {
        _QSCD_FOR_BIN data;
        _QSCD_FOR_MULTIMAP qfmm = qscd20List.at(i);
        data.time = dt;
        qsnprintf(data.netSta, sizeof(data.netSta), "%s", qfmm.netSta.toUtf8().constData());
        for(int j=0;j<5;j++)
            data.pga[j] = qfmm.pga[j];
        stream.writeRawData((char*)&data, sizeof(_QSCD_FOR_BIN));
    }
    qscd20BinFile.close();

    QFile qscd100BinFile(dataFilePath + "/SRC/PGA100.dat");
    qscd100BinFile.open(QIODevice::WriteOnly | QIODevice::Append);
    QDataStream stream2(&qscd100BinFile);

    for(int i=0;i<qscd100List.size();i++)
    {
        _QSCD_FOR_BIN data;
        _QSCD_FOR_MULTIMAP qfmm = qscd100List.at(i);
        data.time = dt;
        qsnprintf(data.netSta, sizeof(data.netSta), "%s", qfmm.netSta.toUtf8().constData());
        for(int j=0;j<5;j++)
            data.pga[j] = qfmm.pga[j];
        stream2.writeRawData((char*)&data, sizeof(_QSCD_FOR_BIN));
    }
    qscd100BinFile.close();
}

void MainClass::doRepeatWork()
{
    QDateTime systemTimeUTC = QDateTime::currentDateTimeUtc();
    QDateTime dataTime = systemTimeUTC.addSecs(- SECNODS_FOR_ALIGN_QSCD);

    if(dataTime.toTime_t() >= eventtime && dataTime.toTime_t() < eventtime + EVENT_DURATION)
    {
        QList<_QSCD_FOR_MULTIMAP> qscd20List = QSCD20_DATA_HOUSE.values(dataTime.toTime_t());
        QList<_QSCD_FOR_MULTIMAP> qscd100List = QSCD100_DATA_HOUSE.values(dataTime.toTime_t());

        savePGAtoFile(dataTime.toTime_t(), qscd20List, qscd100List);
    }

    mutex.lock();

    if(!QSCD20_DATA_HOUSE.isEmpty())
    {
        QMultiMap<int, _QSCD_FOR_MULTIMAP>::iterator iter;
        QMultiMap<int, _QSCD_FOR_MULTIMAP>::iterator untilIter;
        untilIter = QSCD20_DATA_HOUSE.lowerBound(systemTimeUTC.toTime_t() - KEEP_SMALL_DATA_DURATION);

        for(iter = QSCD20_DATA_HOUSE.begin() ; untilIter != iter;)
        {
            QMultiMap<int, _QSCD_FOR_MULTIMAP>::iterator thisIter;
            thisIter = iter;
            iter++;
            QSCD20_DATA_HOUSE.erase(thisIter);
        }
    }

    if(!QSCD100_DATA_HOUSE.isEmpty())
    {
        QMultiMap<int, _QSCD_FOR_MULTIMAP>::iterator iter;
        QMultiMap<int, _QSCD_FOR_MULTIMAP>::iterator untilIter;
        untilIter = QSCD100_DATA_HOUSE.lowerBound(systemTimeUTC.toTime_t() - KEEP_SMALL_DATA_DURATION);

        for(iter = QSCD100_DATA_HOUSE.begin() ; untilIter != iter;)
        {
            QMultiMap<int, _QSCD_FOR_MULTIMAP>::iterator thisIter;
            thisIter = iter;
            iter++;
            QSCD100_DATA_HOUSE.erase(thisIter);
        }
    }

    mutex.unlock();
}

void MainClass::openDB()
{
    if(!qscdDB.isOpen())
    {
        if(!qscdDB.open())
        {
            writeLog(cfg.logDir, cfg.processName, "Error connecting to DB: " + qscdDB.lastError().text());
        }
    }
}

void MainClass::readStationListfromDB()
{
    staList.clear();

    QString query;
    query = "SELECT * FROM NETWORK";
    openDB();
    networkModel->setQuery(query);

    if(networkModel->rowCount() > 0)
    {
        for(int i=0;i<networkModel->rowCount();i++)
        {
            QString network = networkModel->record(i).value("net").toString();
            query = "SELECT * FROM AFFILIATION where net='" + network + "'";
            affiliationModel->setQuery(query);

            for(int j=0;j<affiliationModel->rowCount();j++)
            {
                QString affiliation = affiliationModel->record(j).value("aff").toString();
                QString affiliationName = affiliationModel->record(j).value("affname").toString();
                float lat = affiliationModel->record(j).value("lat").toDouble();
                float lon = affiliationModel->record(j).value("lon").toDouble();
                query = "SELECT * FROM SITE where aff='" + affiliation + "'";
                siteModel->setQuery(query);

                for(int k=0;k<siteModel->rowCount();k++)
                {
                    _STATION sta;
                    QString staS = siteModel->record(k).value("sta").toString();
                    strcpy(sta.netSta, staS.toLatin1().constData());
                    sta.lat = lat;
                    sta.lon = lon;
                    sta.inUse = siteModel->record(k).value("inuse").toInt();
                    if(sta.inUse != 1)
                        continue;
                    staList.append(sta);
                }
            }
        }
    }

    writeLog(cfg.logDir, cfg.processName, "Succedd Reading Station List from Database");
    writeLog(cfg.logDir, cfg.processName, "The number of the Stations : " + QString::number(staList.size()));

    qscdDB.close();
}
