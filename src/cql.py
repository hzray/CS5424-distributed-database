def select_one(session, query, args):
    return session.execute(query, args).one()


def select(session, query, args):
    return session.execute(query, args)


def update(session, query, args):
    return session.execute(query, args).one()


def insert(session, query, args):
    session.execute(query, args)
