# -*- coding: utf-8 -*-

def decodeColorInline(str):
    str = str.strip()
    if "color" in str:
        return True
    elif "black" in str and "white" in str:
        return False
    elif len(str) > 5:
        return True
    else:
        return False

def decodeColorIsCover(str):
    return "cover" in str

def decodeColorDirtyStr(input):
    split = input.split(";")
    if len(split) == 2:
        if (decodeColorIsCover(split[0])):
            return (decodeColorInline(split[0]), decodeColorInline(split[1]))
        else:
            return (decodeColorInline(split[1]), decodeColorInline(split[0]))
    else:
        ret = decodeColorInline(split[0])
        return (ret, ret)

#################

def extractDetail(input):
    counter = 0
    for s in input:
        if s == '(':
            break
        counter += 1
    name = input[0:counter].strip()
    info = input[counter + 1:-1]
    if len(info) > 2:
        return (name, info)
    else:
        return (name, "")
