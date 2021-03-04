#ifndef MAINCLASS_H
#define MAINCLASS_H

#include <QObject>
#include <QTimer>
#include <QMutex>
#include <QProcess>
#include <QDir>
#include <QDataStream>

#include <QSqlDatabase>
#include <QSqlQuery>
#include <QSqlQueryModel>
#include <QSqlRecord>
#include <QSqlError>

#include "KGEEWLIBS_global.h"
#include "kgeewlibs.h"

#define EEWCORE_VERSION 0.1

static QString find_loc_program = "/home/sysop/KGEEW/BIN/findLocC";

class MainClass : public QObject
{
    Q_OBJECT
public:
    explicit MainClass(QString conFile = nullptr, QObject *parent = nullptr);

private:
    _CONFIGURE cfg;
    RecvEEWMessage *rvEEW_Thread;
    RecvQSCD20Message *rvQSCD20_KISS_Thread;
    RecvQSCD20Message *rvQSCD20_MPSS_Thread;
    RecvQSCD100Message *rvQSCD100_Thread;

    QMutex mutex;

    // About Database & table
    QSqlDatabase qscdDB;
    QSqlQueryModel *eventModel;
    QSqlQueryModel *networkModel;
    QSqlQueryModel *affiliationModel;
    QSqlQueryModel *siteModel;

    void openDB();
    void readStationListfromDB();

    QMultiMap<int, _QSCD_FOR_MULTIMAP> QSCD20_DATA_HOUSE;
    QMultiMap<int, _QSCD_FOR_MULTIMAP> QSCD100_DATA_HOUSE;
    QList<_STATION> staList;

    int eventtime;
    QString dataFilePath;
    void savePGAtoFile(int dataTime, QList<_QSCD_FOR_MULTIMAP> qscd20List, QList<_QSCD_FOR_MULTIMAP> qscd100List);

    void runSQL(QString, QString);
    QString getLocation(QString, QString);

    projPJ pj_eqc;
    projPJ pj_longlat;
    void initProj();

private slots:
    void rvEEWInfo(_EEWINFO);
    void doRepeatWork();
    void extractQSCD20(QMultiMap<int, _QSCD_FOR_MULTIMAP>);
    void extractQSCD100(QMultiMap<int, _QSCD_FOR_MULTIMAP>);
};

#endif // MAINCLASS_H
