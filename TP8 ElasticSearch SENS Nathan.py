from elasticsearch import Elasticsearch

# pip3 install elasticsearch
elastic_client = Elasticsearch(hosts=["localhost"])

"""TP4"""


def research_all_woman():
    search_param = {
        "query": {
            "match": {
                "gender": {
                    "query": "female"
                }
            }
        }
    }
    response = elastic_client.search(index="person-v3", body=search_param)
    print('response all woman: ', response)


def research_ages():
    search_param = {
        "query": {
            "range": {
                "age": {
                    "gte": 20
                }
            }
        }
    }
    response = elastic_client.search(index="person-v3", body=search_param)
    print('response ages: ', response)


def research_males_ages():
    search_param = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "gender": "male"
                        }
                    },
                    {
                        "range": {
                            "age": {
                                "gte": 20
                            }
                        }
                    }
                ]
            }
        }
    }
    response = elastic_client.search(index="person-v3", body=search_param)
    print('response male ages: ', response)


def research_balance_age():
    search_param = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "age": {
                                "gte": 20
                            }
                        }
                    },
                    {
                        "range": {
                            "balance": {
                                "gte": 1000,
                                "lte": 2000
                            }
                        }
                    }
                ]
            }
        }
    }
    response = elastic_client.search(index="person-v3", body=search_param)
    print('response balance age: ', response)


def research_distance_paris():
    search_param = {
        "query": {
            "bool": {
                "must": {
                    "match_all": {}
                },
                "filter": {
                    "geo_distance": {
                        "distance": "20km",
                        "location": {
                            "lat": 48,
                            "lon": 2
                        }
                    }
                }
            }
        }
    }
    response = elastic_client.search(index="person-v3", body=search_param)
    print('response distance paris: ', response)


"""TP5"""


def agregation_age_moyen():
    search_param = {
        "aggs": {
            "age_moyen": {
                "avg": {
                    "field": "age"
                }
            }
        }
    }
    response = elastic_client.search(index="person-v3", body=search_param)
    print('aggregation age recherche: ', response)


def agregation_age_moyen_par_genre():
    search_param = {
        "aggs": {
            "age_moyen": {
                "avg": {
                    "field": "age"
                }
            },
            "par_genre": {
                "terms": {
                    "field": "gender"
                }
            }
        }
    }
    response = elastic_client.search(index="person-v3", body=search_param)
    print('aggregation age par genre recherche: ', response)


def agregation_age_moyen_par_genre_par_couleur():
    search_param = {
        "aggs": {
            "age_moyen": {
                "avg": {
                    "field": "age"
                }
            },
            "par_genre": {
                "terms": {
                    "field": "gender"
                },
                "aggs": {
                    "par_couleurs": {
                        "terms": {
                            "field": "eyeColor"
                        }
                    }
                }
            }
        }
    }
    response = elastic_client.search(index="person-v3", body=search_param)
    print('aggregation age par genre et couleur des yeux recherche: ', response)


def agregation_age_moyen_par_genre_par_couleur_et_annee():
    search_param = {
        "aggs": {
            "age_moyen": {
                "avg": {
                    "field": "age"
                }
            },
            "par_genre": {
                "terms": {
                    "field": "gender"
                },
                "aggs": {
                    "par_couleurs": {
                        "terms": {
                            "field": "eyeColor"
                        }
                    },
                    "par_annee": {
                        "date_histogram": {
                            "field": "registered",
                            "interval": "year"
                        }
                    }
                }
            }
        }
    }
    response = elastic_client.search(index="person-v3", body=search_param)
    print('aggregation age par genre/couleur des yeux et annee recherche: ', response)


if __name__ == '__main__':
    research_all_woman()
    research_ages()
    research_males_ages()
    research_balance_age()
    research_distance_paris()

    agregation_age_moyen()
    agregation_age_moyen_par_genre()
    agregation_age_moyen_par_genre_par_couleur()
    agregation_age_moyen_par_genre_par_couleur_et_annee()
