# -*- coding: utf-8 -*-

import csv
import re

import helper

def createMappingStoryGenre(mapping):
    bufferInput = open("dirty_database/story.csv", 'r')
    bufferMapping = open("dirty_database/storyId_genre.csv", 'w')
    bufferMapping.write("sid,gid")
    rows = bufferInput.readlines()[1:]
    for row in rows:
        array = row.split(",")
        genres = array[10]
        if (len(genres) > 0):
            genresSplit = genres.split(";")
            tmp = []
            for g in genresSplit:
                g = g.strip().lower()
                if len(g) > 1 and g not in tmp:
                    tmp.append(g)
                    bufferMapping.write("\n" + array[0] + "," + str(mapping[g]))
    bufferInput.close()
    bufferMapping.close()

def createGenres():
    bufferInput = open("dirty_database/story.csv", 'r')
    allGenres = []
    rows = bufferInput.readlines()[1:]
    for row in rows:
        array = row.split(",")
        genres = array[10]
        if (len(genres) > 0):
            genresSplit = genres.split(";")
            for g in genresSplit:
                g = g.strip().lower()
                if g not in allGenres and len(g) > 1:
                    allGenres.append(g)
    bufferInput.close()
    bufferGenre = open("dirty_database/story_genre.csv", 'w')
    allGenres = sorted(allGenres, key = str.lower)
    bufferGenre.write("gid,name")
    gid = 1
    dictionaryGenre = {}
    for genre in allGenres:
        bufferGenre.write("\n" + str(gid) + "," + genre)
        dictionaryGenre.update({genre: gid})
        gid += 1
    bufferGenre.close()
    return dictionaryGenre

def createFeatures():
    bufferInput = open("dirty_database/story.csv", 'r')
    dictionaryFeatures = {}
    rows = bufferInput.readlines()[1:]
    for row in rows:
        array = row.split(",")
        features = array[2]
        if (len(features) > 0):
            featuresSplit = features.split("; ")
            for f in featuresSplit:
                f = re.sub(r'\(.*\)', '', f).strip()
                f = f.replace('"', "")
                if (dictionaryFeatures.has_key(f) == False and len(f) > 0 and len(f) <= 255):
                    tmp = len(dictionaryFeatures) + 1
                    dictionaryFeatures[f] = tmp
    bufferInput.close()
    bufferFeatures = open("dirty_database/feature.csv", 'w')
    bufferFeatures.write("fid,name")
    for (f, id) in dictionaryFeatures.iteritems():
        bufferFeatures.write("\n" + str(id) + "," + f)
    bufferFeatures.close()

    ####

    bufferStory = open("dirty_database/story.csv", 'r')
    bufferMapping = open("dirty_database/storyId_featureId.csv", 'w')
    bufferMapping.write("sid,fid")
    rows = bufferStory.readlines()[1:]
    for row in rows:
        array = row.split(",")
        features = array[2]
        if (len(features) > 0):
            featuresSplit = features.split("; ")
            for f in featuresSplit:
                f = re.sub(r'\(.*\)', '', f).strip()
                f = f.replace('"', "")
                if (len(f) > 0 and len(f) <= 255):
                    bufferMapping.write("\n" + array[0] + "," + str(dictionaryFeatures[f]))
    bufferMapping.close()

def createCharacters():
    bufferInput = open("dirty_database/story.csv", 'r')
    dictionaryCharacters = {}
    rows = bufferInput.readlines()[1:]
    for row in rows:
        array = row.split(",")
        characters = array[11]
        if (len(characters ) > 0):
            charactersSplit = characters.split("; ")
            for c in charactersSplit:
                c = re.sub(r'\(.*\)', '', c).strip()
                c = c.replace('"', "")
                if (dictionaryCharacters.has_key(c) == False and len(c) > 1 and len(c) <= 255):
                    tmp = len(dictionaryCharacters) + 1
                    dictionaryCharacters[c] = tmp
    bufferInput.close()
    bufferCharacters = open("dirty_database/characters.csv", 'w')
    bufferCharacters.write("cid,name")
    for (c, id) in dictionaryCharacters.iteritems():
        bufferCharacters.write("\n" + str(id) + "," + c)
    bufferCharacters.close()

    ####

    bufferStory = open("dirty_database/story.csv", 'r')
    bufferMapping = open("dirty_database/storyId_characterId.csv", 'w')
    bufferMapping.write("sid,cid")
    rows = bufferStory.readlines()[1:]
    for row in rows:
        array = row.split(",")
        characters = array[11]
        if (len(characters) > 0):
            charactersSplit = characters.split("; ")
            tmp = []
            for c in charactersSplit:
                c = re.sub(r'\(.*\)', '', c).strip()
                c = c.replace('"', "")
                if (c not in tmp and len(c) > 1 and len(c) <= 255):
                    tmp.append(c)
                    bufferMapping.write("\n" + array[0] + "," + str(dictionaryCharacters[c]))
    bufferMapping.close()

def createPersons():
    bufferInput = open("dirty_database/story.csv", 'r')
    bufferPersons = open("dirty_database/persons.csv", 'w')
    bufferPersons.write("pid,name")
    bufferStoryParticipation = open("dirty_database/story_participation.csv", 'w')
    dictionaryPersons = {}
    rows = bufferInput.readlines()[1:]
    for rowString in rows:
        row = rowString.split(",")
        sid = row[0]
        script = row[4]
        pencils = row[5]
        inks = row[6]
        color = row[7]
        letters = row[8]
        #####

        persons = script.split("; ")
        for p in persons:
            if (len(p) > 1 and dictionaryPersons.has_key(p) == False):
                tmp = len(dictionaryPersons) + 1
                dictionaryPersons[p] = tmp
                bufferPersons.write("\n" + str(tmp) + "," + p)
                bufferStoryParticipation.write("\n" + sid + "," + "1," + str(tmp))
        persons = pencils.split("; ")
        for p in persons:
            if (len(p) > 1 and dictionaryPersons.has_key(p) == False):
                tmp = len(dictionaryPersons) + 1
                dictionaryPersons[p] = tmp
                bufferPersons.write("\n" + str(tmp) + "," + p)
                bufferStoryParticipation.write("\n" + sid + "," + "2," + str(tmp))
        persons = inks.split("; ")
        for p in persons:
            if (len(p) > 1 and dictionaryPersons.has_key(p) == False):
                tmp = len(dictionaryPersons) + 1
                dictionaryPersons[p] = tmp
                bufferPersons.write("\n" + str(tmp) + "," + p)
                bufferStoryParticipation.write("\n" + sid + "," + "3," + str(tmp))
        persons = color.split("; ")
        for p in persons:
            if (len(p) > 1 and dictionaryPersons.has_key(p) == False):
                tmp = len(dictionaryPersons) + 1
                dictionaryPersons[p] = tmp
                bufferPersons.write("\n" + str(tmp) + "," + p)
                bufferStoryParticipation.write("\n" + sid + "," + "4," + str(tmp))
        persons = letters.split("; ")
        for p in persons:
            if (len(p) > 1 and dictionaryPersons.has_key(p) == False):
                tmp = len(dictionaryPersons) + 1
                dictionaryPersons[p] = tmp
                bufferPersons.write("\n" + str(tmp) + "," + p)
                bufferStoryParticipation.write("\n" + sid + "," + "5," + str(tmp))
    bufferInput.close()
    bufferStoryParticipation.close()
    ########
    bufferInput = open("dirty_database/issue.csv", 'r')
    bufferEditingParticipation = open("dirty_database/editing_participation.csv", 'w')
    bufferEditingParticipation.write("iid,aid,pid")
    rows = bufferInput.readlines()[1:]
    for rowString in rows:
        row = rowString.split(",")
        iid = row[0]
        item = row[8]
        persons = item.split("; ")
        for person in persons:
            (p, detail) = helper.extractDetail(person)
            if len(p) > 1 and dictionaryPersons.has_key(p) == False:
                tmp = len(dictionaryPersons) + 1
                dictionaryPersons[p] = tmp
                bufferPersons.write("\n" + str(tmp) + "," + p)
                bufferEditingParticipation.write("\n" + iid + ",6," + str(tmp) + "," + detail)
    bufferInput.close()
    bufferPersons.close()
    bufferEditingParticipation.close()

def init():
    genreGid = createGenres()
    createMappingStoryGenre(genreGid)
    createFeatures()
    createCharacters()
    createPersons()
