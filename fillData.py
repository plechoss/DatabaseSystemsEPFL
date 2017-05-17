# -*- coding: utf-8 -*-

import psycopg2
import sys

import csv

import helper

def fillMappingIssuePerson():
    bufferIssue = open("dirty_database/editing_participation.csv", 'r')
    con = None
    try:
        dbName = "test_python"
        userName = "ginger"
        cmdSql = """INSERT INTO mappingissueperson(iid, aid, pid, details)
        VALUES(%s, %s, %s, %s)"""
        con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
        cur = con.cursor()
        rows = bufferIssue.readlines()[1:]
        for rowString in rows:
            row = rowString.split(",")
            iid = row[0]
            aid = row[1]
            pid = row[2]
            details = row[3]
            if len(details) == 0:
                details = None
            cur.execute(cmdSql, (iid, aid, pid, details))
        con.commit()
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            print e
    finally:
        if con:
            con.close()

def fillMappingIssueReprint():
    bufferIssue = open("dirty_database/issue_reprint.csv", 'r')
    con = None
    try:
        dbName = "test_python"
        userName = "ginger"
        cmdSql = """INSERT INTO mappingissuereprint(rid, origin_iid, target_iid)
        VALUES(%s, %s, %s)"""
        con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
        cur = con.cursor()
        rows = bufferIssue.readlines()[1:]
        for rowString in rows:
            row = rowString.split(",")
            rid = row[0]
            sid = row[1]
            aid = row[2]
            cur.execute(cmdSql, (rid, sid, aid))
        con.commit()
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            print e
    finally:
        if con:
            con.close()

def fillMappingStoryReprint():
    bufferIssue = open("dirty_database/story_reprint.csv", 'r')
    con = None
    try:
        dbName = "test_python"
        userName = "ginger"
        cmdSql = """INSERT INTO mappingstoryreprint(rid, origin_sid, target_sid)
        VALUES(%s, %s, %s)"""
        con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
        cur = con.cursor()
        rows = bufferIssue.readlines()[1:]
        for rowString in rows:
            row = rowString.split(",")
            rid = row[0]
            sid = row[1]
            aid = row[2]
            cur.execute(cmdSql, (rid, sid, aid))
        con.commit()
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            print e
    finally:
        if con:
            con.close()

def fillMappingStoryPerson():
    bufferIssue = open("dirty_database/story_participation.csv", 'r')
    con = None
    try:
        dbName = "test_python"
        userName = "ginger"
        cmdSql = """INSERT INTO mappingstoryperson(sid, aid, pid)
        VALUES(%s, %s, %s)"""
        con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
        cur = con.cursor()
        rows = bufferIssue.readlines()[1:]
        for rowString in rows:
            row = rowString.split(",")
            sid = row[0]
            aid = row[1]
            pid = row[2]
            cur.execute(cmdSql, (sid, aid, pid))
        con.commit()
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            print e
    finally:
        if con:
            con.close()
    bufferIssue.close()

def fillMappingStoryGenre():
    bufferIssue = open("dirty_database/storyId_genre.csv", 'r')
    con = None
    try:
        dbName = "test_python"
        userName = "ginger"
        cmdSql = """INSERT INTO mappingstorygenre(sid, sgid)
        VALUES(%s, %s)"""
        con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
        cur = con.cursor()
        rows = bufferIssue.readlines()[1:]
        for rowString in rows:
            row = rowString.split(",")
            sid = row[0]
            cid = row[1]
            cur.execute(cmdSql, (sid, cid))
        con.commit()
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            print e
    finally:
        if con:
            con.close()
    bufferIssue.close()

def fillMappingStoryFeature():
    bufferIssue = open("dirty_database/storyId_featureId.csv", 'r')
    con = None
    try:
        dbName = "test_python"
        userName = "ginger"
        cmdSql = """INSERT INTO mappingstoryfeature(sid, fid)
        VALUES(%s, %s)"""
        con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
        cur = con.cursor()
        rows = bufferIssue.readlines()[1:]
        for rowString in rows:
            row = rowString.split(",")
            sid = row[0]
            cid = row[1]
            cur.execute(cmdSql, (sid, cid))
        con.commit()
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            print e
    finally:
        if con:
            con.close()
    bufferIssue.close()

def fillMappingStoryCharacter():
    bufferIssue = open("dirty_database/storyId_characterId.csv", 'r')
    con = None
    try:
        dbName = "test_python"
        userName = "ginger"
        cmdSql = """INSERT INTO mappingstorycharacter(sid, cid)
        VALUES(%s, %s)"""
        con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
        cur = con.cursor()
        rows = bufferIssue.readlines()[1:]
        for rowString in rows:
            row = rowString.split(",")
            sid = row[0]
            cid = row[1]
            cur.execute(cmdSql, (sid, cid))
        con.commit()
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            print e
    finally:
        if con:
            con.close()
    bufferIssue.close()

def fillPerson():
    bufferIssue = open("dirty_database/persons.csv", 'r')
    con = None
    try:
        dbName = "test_python"
        userName = "ginger"
        cmdSql = """INSERT INTO persons(pid, name)
        VALUES(%s, %s)"""
        con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
        cur = con.cursor()
        rows = bufferIssue.readlines()[1:]
        for rowString in rows:
            row = rowString.split(",")
            aid = row[0]
            name = row[1]
            cur.execute(cmdSql, (aid, name))
        con.commit()
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            print e
    finally:
        if con:
            con.close()
    bufferIssue.close()

def fillAction():
    bufferIssue = open("dirty_database/actions.csv", 'r')
    con = None
    try:
        dbName = "test_python"
        userName = "ginger"
        cmdSql = """INSERT INTO actions(aid, name)
        VALUES(%s, %s)"""
        con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
        cur = con.cursor()
        rows = bufferIssue.readlines()[1:]
        for rowString in rows:
            row = rowString.split(",")
            aid = row[0]
            name = row[1]
            cur.execute(cmdSql, (aid, name))
        con.commit()
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            print e
    finally:
        if con:
            con.close()
    bufferIssue.close()

def fillIssue():
    bufferIssue = open("dirty_database/issue.csv", 'r')
    con = None
    try:
        dbName = "test_python"
        userName = "ginger"
        cmdSql = """INSERT INTO issues(iid, title, number, publication_year, isbn, page_count, frequency, price, notes, rating, on_sale_year, sid, ipid)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
        cur = con.cursor()
        rows = bufferIssue.readlines()[1:]
        for rowString in rows:
            row = rowString.split(",")
            iid = row[0]
            title = row[13]
            number = row[1]
            if number.isdigit() == False:
                number = None
            publication_year = ''.join(c for c in row[4] if c.isdigit())
            if publication_year.isdigit() == False or publication_year > 2040:
                publication_year = None
            isbn = row[11]
            if isbn == "":
                isbn = None
            page_count = row[6].replace(".", "")
            if page_count.isdigit() == False:
                page_count = None
            frequency = row[7]
            if frequency == "":
                frequency = None
            price = row[5]
            if price == "":
                price = None
            notes = row[9]
            if notes == "":
                notes = None
            try:
                rating = row[15].strip()
                if rating.isdigit() == False or int(rating) > 9853019:
                    rating = None
            except IndexError:
                print iid + "---"
                rating = None
            try:
                on_sale_year = row[14]
                if on_sale_year.isdigit() == False:
                    on_sale_year = None
            except IndexError:
                on_sale_year = None
            sid = row[2]
            if sid.isdigit() == False:
                sid = None
            ipid = row[3]
            if ipid.isdigit() == False:
                ipid = None
            cur.execute(cmdSql, (iid, title, number, publication_year, isbn, page_count, frequency, price, notes, rating, on_sale_year, sid, ipid))
        con.commit()
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            print e
    finally:
        if con:
            con.close()
            print str(iid) + ", " + str(number) + ", " + row[10]
    bufferIssue.close()

def fillSerie():
    bufferSerie = open("dirty_database/series.csv", 'r')
    con = None
    try:
        dbName = "test_python"
        userName = "ginger"
        cmdSql = """INSERT INTO series(sid, name, format, notes, cover_color, interior_color, year_began, year_ended,
        dimension_x, dimension_y, paper_stock, binding, publishing_format, fiid, liid, cid, lid, pid, ptid)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
        cur = con.cursor()
        rows = bufferSerie.readlines()[1:]
        for rowString in rows:
            row = rowString.split(",")
            sid = row[0]
            name = row[1]
            format = row[2]
            notes = row[11]
            colorDirty = row[12].lower()
            (cover_color, interior_color) = helper.decodeColorDirtyStr(colorDirty)
            year_began = row[3]
            year_end = row[4]
            if year_end == "NULL":
                year_end = None
            dimensionDirty = row[13]
            dimension_x = 0
            dimension_y = 0
            paper_stock = row[14][:63]
            if len(paper_stock) == 0:
                paper_stock = None
            binding = row[15].split(";")[0]
            publishing_format = row[16]
            fiid = row[6]
            if fiid == "NULL" or fiid == "":
                fiid = None
            liid = row[7]
            if liid == "NULL" or liid == "":
                liid = None
            cid = row[9]
            if cid == "NULL" or cid == "":
                cid = None
            lid = row[10]
            if lid == "NULL" or lid == "":
                lid = None
            pid = row[8]
            if pid == "NULL" or pid == "":
                pid = None
            ptid = row[17].strip()
            if ptid == "NULL" or ptid == "":
                ptid = None
            entry = (cmdSql, (sid, name, format, notes, str(cover_color), str(interior_color), year_began, year_end,
            str(dimension_x), str(dimension_y), paper_stock, binding, publishing_format, fiid, liid, cid, lid, pid, ptid))
            cur.execute(cmdSql, (sid, name, format, notes, str(cover_color), str(interior_color), year_began, year_end,
            str(dimension_x), str(dimension_y), paper_stock, binding, publishing_format, fiid, liid, cid, lid, pid, ptid))
        con.commit()
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            print e
    finally:
        if con:
            con.close()
            print str(sid)
    bufferSerie.close()

def fillStory():
    bufferStory = open("dirty_database/story.csv", 'r')
    con = None
    try:
        dbName = "test_python"
        userName = "ginger"
        cmdSql = """INSERT INTO stories(sid, title, editing, synopsys, notes, reprint_notes, gid, iid, stid)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
        cur = con.cursor()
        rows = bufferStory.readlines()[1:]
        for rowString in rows:
            row = rowString.split(",")
            sid = row[0]
            title = row[1]
            editing = row[9]
            synopsys = row[12]
            reprint_notes = row[13]
            notes = row[14]
            iid = row[3]
            stid = row[15]
            cur.execute(cmdSql, (sid, title, editing, synopsys, notes, reprint_notes, "1", iid, stid))
        con.commit()
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            print e
    finally:
        if con:
            con.close()
    bufferStory.close()

def fillBrandGroup():
    with open("dirty_database/brand_group.csv", "r") as csvfile:
        con = None
        try:
            dbName = "test_python"
            userName = "ginger"
            cmdSql = """INSERT INTO brandgroups(bgid, name, year_began, year_ended, notes, url, pid)
                VALUES(%s, %s, %s, %s, %s, %s, %s)"""
            con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
            buffer = csv.reader(csvfile, delimiter=',')
            buffer.next() #skip the first line
            cur = con.cursor()
            for row in buffer:
                if row[2] == "NULL":
                    row[2] = None
                if row[3] == "NULL":
                    row[3] = None
                cur.execute(cmdSql, (row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
            con.commit()
        except psycopg2.DatabaseError, e:
            if con:
                con.rollback()
                print e
        finally:
            if con:
                con.close()

def fillIndiciaPublisher():
    with open("dirty_database/indicia_publisher.csv", "r") as csvfile:
        con = None
        try:
            dbName = "test_python"
            userName = "ginger"
            cmdSql = """INSERT INTO indiciapublishers(ipid, name, pid, cid, year_began, year_ended, is_surrogate, notes, url)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
            buffer = csv.reader(csvfile, delimiter=',')
            buffer.next() #skip the first line
            cur = con.cursor()
            for row in buffer:
                if row[4] == "NULL":
                    row[4] = None
                if row[5] == "NULL":
                    row[5] = None
                if row[6] == 0:
                    row[6] = False
                else:
                    row[6] = True
                cur.execute(cmdSql, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
            con.commit()
        except psycopg2.DatabaseError, e:
            if con:
                con.rollback()
                print e
        finally:
            if con:
                con.close()

def fillPublisher():
    with open("dirty_database/publisher.csv", "r") as csvfile:
        con = None
        try:
            dbName = "test_python"
            userName = "ginger"
            cmdSql = "INSERT INTO publishers(pid, name, cid, year_began, year_ended, notes, url) VALUES(%s, %s, %s, %s, %s, %s, %s)"
            con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
            buffer = csv.reader(csvfile, delimiter=',')
            buffer.next() #skip the first line
            cur = con.cursor()
            for row in buffer:
                if row[3] == "NULL":
                    row[3] = None
                if row[4] == "NULL":
                    row[4] = None
                cur.execute(cmdSql, (row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
            con.commit()
        except psycopg2.DatabaseError, e:
            if con:
                con.rollback()
                print e
        finally:
            if con:
                con.close()

def fillCharacter():
    bufferCharacters = open("dirty_database/characters.csv", 'r')
    con = None
    try:
        dbName = "test_python"
        userName = "ginger"
        cmdSql = "INSERT INTO characters(cid, name) VALUES(%s, %s)"
        con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
        cur = con.cursor()
        rows = bufferCharacters.readlines()[1:]
        for row in rows:
            array = row.split(",")
            cur.execute(cmdSql, (array[0], array[1]))
        con.commit()
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            print e
    finally:
        if con:
            con.close()
    bufferCharacters.close()

def fillFeature():
    with open("dirty_database/feature.csv", "r") as csvfile:
        con = None
        try:
            dbName = "test_python"
            userName = "ginger"
            cmdSql = "INSERT INTO storyfeature(fid, name) VALUES(%s, %s)"
            con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
            buffer = csv.reader(csvfile, delimiter=',')
            buffer.next() #skip the first line
            cur = con.cursor()
            for row in buffer:
                cur.execute(cmdSql, (row[0], row[1]))
            con.commit()
        except psycopg2.DatabaseError, e:
            if con:
                con.rollback()
                print e
        finally:
            if con:
                con.close()

def fillStoryGenre():
    with open("dirty_database/story_genre.csv", "r") as csvfile:
        con = None
        try:
            dbName = "test_python"
            userName = "ginger"
            cmdSql = "INSERT INTO storygenre(sgid, name) VALUES(%s, %s)"
            con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
            buffer = csv.reader(csvfile, delimiter=',')
            buffer.next() #skip the first line
            cur = con.cursor()
            for row in buffer:
                cur.execute(cmdSql, (row[0], row[1]))
            con.commit()
        except psycopg2.DatabaseError, e:
            if con:
                con.rollback()
                print e
        finally:
            if con:
                con.close()

def fillStoryType():
    with open("dirty_database/story_type.csv", "r") as csvfile:
        con = None
        try:
            dbName = "test_python"
            userName = "ginger"
            cmdSql = "INSERT INTO storytype(stid, name) VALUES(%s, %s)"
            con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
            buffer = csv.reader(csvfile, delimiter=',')
            buffer.next() #skip the first line
            cur = con.cursor()
            for row in buffer:
                cur.execute(cmdSql, (row[0], row[1]))
            con.commit()
        except psycopg2.DatabaseError, e:
            if con:
                con.rollback()
                print e
        finally:
            if con:
                con.close()

def fillSeriePublicationType():
    with open("dirty_database/series_publication_type.csv", "r") as csvfile:
        con = None
        try:
            dbName = "test_python"
            userName = "ginger"
            cmdSql = "INSERT INTO seriespublicationtype(spid, name) VALUES(%s, %s)"
            con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
            buffer = csv.reader(csvfile, delimiter=',')
            buffer.next() #skip the first line
            cur = con.cursor()
            for row in buffer:
                cur.execute(cmdSql, (row[0], row[1]))
            con.commit()
        except psycopg2.DatabaseError, e:
            if con:
                con.rollback()
                print e
        finally:
            if con:
                con.close()

def fillCountries():
    with open("dirty_database/country.csv", "r") as csvfile:
        con = None
        try:
            dbName = "test_python"
            userName = "ginger"
            cmdSql = "INSERT INTO countries(cid, ccode, name) VALUES(%s, %s, %s)"
            con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
            buffer = csv.reader(csvfile, delimiter=',')
            buffer.next() #skip the first line
            cur = con.cursor()
            for row in buffer:
                cur.execute(cmdSql, (row[0], row[1], row[2]))
            con.commit()
        except psycopg2.DatabaseError, e:
            if con:
                con.rollback()
                print e
        finally:
            if con:
                con.close()

def fillLanguages():
    with open("dirty_database/language.csv", "r") as csvfile:
        con = None
        try:
            dbName = "test_python"
            userName = "ginger"
            cmdSql = "INSERT INTO languages(lid, lcode, name) VALUES(%s, %s, %s)"
            con = psycopg2.connect("dbname='" + dbName + "' user='" + userName +"'")
            buffer = csv.reader(csvfile, delimiter=',')
            buffer.next() #skip the first line
            cur = con.cursor()
            for row in buffer:
                cur.execute(cmdSql, (row[0], row[1], row[2]))
            con.commit()
        except psycopg2.DatabaseError, e:
            if con:
                con.rollback()
                print e
        finally:
            if con:
                con.close()

def init():
    fillLanguages()
    fillCountries()
    fillSeriePublicationType()
    fillStoryType()
    fillStoryGenre()
    fillFeature()
    fillCharacter()
    fillPublisher()
    fillIndiciaPublisher()
    fillBrandGroup()
    fillStory()
    fillSerie()
    fillIssue()
    fillAction()
    fillPerson()
    fillMappingStoryCharacter()
    fillMappingStoryFeature()
    fillMappingStoryGenre()
    fillMappingStoryPerson()
    fillMappingStoryReprint()
    fillMappingIssueReprint()
    fillMappingIssuePerson()
