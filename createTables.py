# -*- coding: utf-8 -*-

import psycopg2
import sys

def createTable(cmdSql):
    con = None
    try:
        dbName = "test_python"
        userName = "ginger"
        con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
        cur = con.cursor()
        cur.execute(cmdSql)
        con.commit()
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
        #print e
    finally:
        if con:
            con.close()

def initCreation():
    languageSql = """
        CREATE TABLE Languages(
            lid INTEGER,
            lcode CHAR(3),
            name VARCHAR(31),
            PRIMARY KEY(lid),
            UNIQUE(name)
        )
    """
    countrySql = """
        CREATE TABLE Countries(
            cid INTEGER,
            ccode VARCHAR(4),
            name VARCHAR(36),
            PRIMARY KEY(cid),
            UNIQUE(name)
        )
    """
    seriePublicationTypeSql = """
        CREATE TABLE SeriesPublicationType(
            spid INTEGER,
            name VARCHAR(16),
            PRIMARY KEY(spid),
            UNIQUE(name)
        )
    """
    storyTypeSql = """
        CREATE TABLE StoryType(
            stid INTEGER,
            name VARCHAR(40),
            PRIMARY KEY(stid),
            UNIQUE(name)
        )
    """
    storyGenreSql = """
        CREATE TABLE StoryGenre(
            sgid INTEGER,
            name VARCHAR(40),
            PRIMARY KEY(sgid),
            UNIQUE(name)
        )
    """
    featureSql = """
        CREATE TABLE StoryFeature(
            fid INTEGER,
            name VARCHAR(255),
            PRIMARY KEY(fid),
            UNIQUE(name)
        )
    """
    characterSql = """
        CREATE TABLE Characters(
            cid INTEGER,
            name VARCHAR(255),
            PRIMARY KEY(cid)
        )
    """
    publisherSql = """
        CREATE TABLE Publishers(
            pid INTEGER,
            name VARCHAR(127),
            cid INTEGER,
            year_began INTEGER,
            year_ended INTEGER,
            notes VARCHAR(3500),
            url VARCHAR(255),
            PRIMARY KEY(pid),
            FOREIGN KEY (cid) REFERENCES countries ON DELETE SET NULL
        )
    """
    indiciaPublisherSql = """
        CREATE TABLE IndiciaPublishers(
            ipid INTEGER,
            name VARCHAR(127),
            pid INTEGER NOT NULL,
            cid INTEGER NOT NULL,
            year_began INTEGER,
            year_ended INTEGER,
            is_surrogate BOOLEAN,
            notes TEXT,
            url VARCHAR(127),
            PRIMARY KEY(ipid),
            FOREIGN KEY (pid) REFERENCES Publishers ON DELETE SET NULL,
            FOREIGN KEY (cid) REFERENCES Countries ON DELETE SET NULL
        )
    """
    brandGroupSql = """
        CREATE TABLE BrandGroups(
            bgid INTEGER,
            name VARCHAR(127),
            year_began INTEGER,
            year_ended INTEGER,
            notes VARCHAR(2047),
            url VARCHAR(255),
            pid INTEGER,
            PRIMARY KEY(bgid),
            FOREIGN KEY (pid) REFERENCES Publishers ON DELETE SET NULL
        )
    """
    storySql = """
        CREATE TABLE Stories(
            sid INTEGER,
            title VARCHAR(255),
            editing TEXT,
            synopsys TEXT,
            reprint_notes TEXT,
            notes TEXT,
            gid INTEGER,
            iid INTEGER,
            stid INTEGER,
            PRIMARY KEY (sid)
        )
    """
    serieSql = """
        CREATE TABLE Series(
            sid INTEGER,
            name VARCHAR(255),
            format VARCHAR(255),
            notes TEXT,
            cover_color BOOLEAN,
            interior_color BOOLEAN,
            year_began INTEGER,
            year_ended INTEGER,
            dimension_x INTEGER,
            dimension_y INTEGER,
            paper_stock VARCHAR(255),
            binding VARCHAR(255),
            publishing_format VARCHAR(255),
            fiid INTEGER,
            liid INTEGER,
            cid INTEGER,
            lid INTEGER,
            pid INTEGER,
            ptid INTEGER,
            PRIMARY KEY(sid) -- !!!!!!!!!! Foreign constraint to be done
        )
    """
    issueSql = """
        CREATE TABLE Issues(
            iid INTEGER,
            title VARCHAR(255),
            number INTEGER,
            publication_year INTEGER,
            isbn VARCHAR(15),
            page_count INTEGER,
            frequency VARCHAR(250),
            price VARCHAR(255),
            notes TEXT,
            rating INTEGER,
            on_sale_year INTEGER, sid INTEGER, ipid INTEGER,
            PRIMARY KEY (iid),
            FOREIGN KEY (sid) REFERENCES Series (sid) ON DELETE SET NULL,
            FOREIGN KEY (ipid) REFERENCES IndiciaPublishers (ipid) ON DELETE SET NULL
        );
    """
    actionSql = """
        CREATE TABLE Actions(
            aid INTEGER,
            name VARCHAR(15),
            PRIMARY KEY (aid),
            UNIQUE(name)
        );
    """
    personSql = """
        CREATE TABLE Persons(
            pid INTEGER,
            name VARCHAR(255),
            PRIMARY KEY (pid),
            UNIQUE(name)
        );
    """
    mappingStoryCharacterSql = """
        CREATE TABLE MappingStoryCharacter(
            sid INTEGER,
            cid INTEGER,
            PRIMARY KEY (sid, cid),
            FOREIGN KEY (sid) REFERENCES Stories(sid) ON DELETE CASCADE,
            FOREIGN KEY (cid) REFERENCES Characters(cid) ON DELETE CASCADE
        );
    """
    mappingStoryFeatureSql = """
        CREATE TABLE MappingStoryFeature(
            sid INTEGER,
            fid INTEGER,
            PRIMARY KEY (sid, fid),
            FOREIGN KEY (sid) REFERENCES Stories(sid) ON DELETE CASCADE,
            FOREIGN KEY (fid) REFERENCES StoryFeature(fid) ON DELETE CASCADE
        );
    """
    mappingStoryGenreSql = """
        CREATE TABLE MappingStoryGenre(
            sid INTEGER,
            sgid INTEGER,
            PRIMARY KEY (sid, sgid),
            FOREIGN KEY (sid) REFERENCES Stories(sid) ON DELETE CASCADE,
            FOREIGN KEY (sgid) REFERENCES StoryGenre(sgid) ON DELETE CASCADE
        );
    """
    mappingStoryPersonSql = """
        CREATE TABLE MappingStoryPerson(
            sid INTEGER,
            aid INTEGER,
            pid INTEGER,
            PRIMARY KEY (sid, aid, pid),
            FOREIGN KEY (sid) REFERENCES Stories(sid) ON DELETE CASCADE,
            FOREIGN KEY (aid) REFERENCES Actions(aid) ON DELETE CASCADE,
            FOREIGN KEY (pid) REFERENCES Persons(pid) ON DELETE CASCADE
        );
    """
    mappingStoryReprintSql = """
        CREATE TABLE MappingStoryReprint(
            rid INTEGER,
            origin_sid INTEGER,
            target_sid INTEGER,
            PRIMARY KEY (rid),
            FOREIGN KEY (origin_sid) REFERENCES Stories(sid) ON DELETE CASCADE,
            FOREIGN KEY (target_sid) REFERENCES Stories(sid) ON DELETE CASCADE
        );
    """
    mappingIssueReprintSql = """
        CREATE TABLE MappingIssueReprint(
            rid INTEGER,
            origin_iid INTEGER,
            target_iid INTEGER,
            PRIMARY KEY (rid),
            FOREIGN KEY (origin_iid) REFERENCES Issues(iid) ON DELETE CASCADE,
            FOREIGN KEY (target_iid) REFERENCES Issues(iid) ON DELETE CASCADE
        );
    """
    mappingIssuePersonSql = """
        CREATE TABLE MappingIssuePerson(
            iid INTEGER,
            aid INTEGER,
            pid INTEGER,
            details VARCHAR(255),
            PRIMARY KEY (iid, aid, pid),
            FOREIGN KEY (iid) REFERENCES Issues(iid) ON DELETE CASCADE,
            FOREIGN KEY (aid) REFERENCES Actions(aid) ON DELETE CASCADE,
            FOREIGN KEY (pid) REFERENCES Persons(pid) ON DELETE CASCADE
        );
    """
    cmdCreations = [languageSql,
                    countrySql,
                    seriePublicationTypeSql,
                    storyTypeSql,
                    storyGenreSql,
                    featureSql,
                    characterSql,
                    publisherSql,
                    indiciaPublisherSql,
                    brandGroupSql,
                    storySql,
                    serieSql,
                    issueSql,
                    actionSql,
                    personSql,
                    mappingStoryCharacterSql,
                    mappingStoryFeatureSql,
                    mappingStoryGenreSql,
                    mappingStoryPersonSql,
                    mappingStoryReprintSql,
                    mappingIssueReprintSql,
                    mappingIssuePersonSql]
    for cmd in cmdCreations:
        createTable(cmd)
