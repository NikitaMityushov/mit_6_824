module github.com/NikitaMityushov/mit_6_824/labs/kvserver

go 1.22.2

require (
	github.com/NikitaMityushov/mit_6_824/labs/labrpc v1.0.0
	github.com/NikitaMityushov/mit_6_824/labs/models v1.0.0
	github.com/NikitaMityushov/mit_6_824/labs/porcupine v1.0.0
)

require github.com/google/uuid v1.6.0 // indirect

replace (
	github.com/NikitaMityushov/mit_6_824/labs/labrpc => ../labrpc
	github.com/NikitaMityushov/mit_6_824/labs/models => ../models
	github.com/NikitaMityushov/mit_6_824/labs/porcupine => ../porcupine
)
