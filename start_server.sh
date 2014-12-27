#!/bin/sh
clear
erl -sname $1@localhost -pa ebin -pa resource_discovery/ebin -setcookie XXXXX -s bootstrap start
