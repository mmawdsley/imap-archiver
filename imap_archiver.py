#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import imaplib
import re
import itertools

class ImapArchiver(object):
    """Archives old messages in IMAP mailboxes."""

    def __init__(self, connection, max_age=365, max_messages=50, archive_mailbox_name="Archives"):
        self.connection = connection
        self.max_age = max_age
        self.max_messages = max_messages
        self.archive_mailbox_name = archive_mailbox_name
        self.archive_mailboxes = self.get_mailboxes_matching(self.archive_mailbox_name)
        self.pattern = re.compile('^[0-9]+ \(UID ([0-9]+) INTERNALDATE "([0-9]+-[a-zA-Z]+-[0-9]+ [0-9]+:[0-9]+:[0-9]+ [-\+][0-9]+)')
        self.now = datetime.datetime.now(datetime.timezone.utc)

    def archive_mailbox(self, mailbox):
        """Archive any messages older than max_age in the given mailbox."""

        self.select_mailbox(mailbox)

        message_uids = self.get_message_uids()

        if not message_uids:
            return

        for start in range(0, len(message_uids), self.max_messages):
            message_uid_row = list(message_uids[start:start+self.max_messages])
            messages = self.get_messages(message_uid_row, mailbox)

            for archive_mailbox, messages in itertools.groupby(messages, key=lambda x:x["mailbox"]):
                message_uid_row = [message["uid"] for message in messages]
                self.archive_messages(message_uid_row, mailbox, archive_mailbox)

    def archive_messages(self, message_uids, mailbox, archive_mailbox):
        """Move the messages to the new mailbox, creating it if needed"""

        try:
            self.archive_mailboxes.index(archive_mailbox)
        except ValueError:
            self.create_archive_mailbox(archive_mailbox)

        message_set = self.build_message_set(message_uids)

        type, data = self.connection.uid("move", message_set, self.quote_mailbox(archive_mailbox))

        if type != "OK":
            raise Exception("Failed to move messages from %s to %s" % (mailbox, archive_mailbox))

    def build_message_set(self, message_uids):
        """Compress the message UIDs into sets"""

        sets = []
        set = None

        for message_uid in message_uids:
            if set is None:
                set = [message_uid]
            elif set and set[-1] == message_uid - 1:
                set.append(message_uid)
            else:
                sets.append(set)
                set = [message_uid]

        sets.append(set)

        return ",".join(map(lambda x: "%s:%s" % (x[0], x[-1]) if len(x) > 1 else str(x[0]), sets))

    def build_archive_mailbox(self, date, mailbox):
        """Build the name of the archive mailbox"""

        mailbox_name = "%s.%d.%s" % (self.archive_mailbox_name, date.year, mailbox)
        return mailbox_name.replace(".INBOX", "")

    def create_archive_mailbox(self, mailbox_name):
        """Create the archive mailbox and add it to the list"""

        self.connection.create(self.quote_mailbox(mailbox_name))
        self.archive_mailboxes.append(mailbox_name)

    def select_mailbox(self, mailbox):
        """Open the given mailbox in read/write mode"""

        try:
            type = self.connection.select(self.quote_mailbox(mailbox), True)[0]
        except Exception as e:
            raise Exception("Mailbox selection threw an exception: %s" % e)

        if type != "OK":
            raise Exception('Could not select mailbox "%s"' % mailbox)

    def quote_mailbox(self, mailbox_name):
        """Double quote in case the mailbox includes spaces"""

        return "\"%s\"" % mailbox_name

    def get_message_uids(self):
        """Return the UIDs of the messages in the current mailbox"""

        max_date = self.now - datetime.timedelta(days=self.max_age + 1)
        query = '(BEFORE "%s")' % max_date.strftime("%d-%b-%Y")
        type, data = self.connection.uid("search", None, query)

        if type != "OK":
            raise Exception('Could not get message IDs from mailbox "%s"' % mailbox)

        message_uids = list(map(int, data[0].split()))
        message_uids.sort()

        return message_uids

    def get_messages(self, message_uids, mailbox):
        """
        Fetch the dates for the given messages and build the name of
        the mailboxes to store them in
        """

        message_set = self.build_message_set(message_uids)

        type, data = self.connection.uid("fetch", message_set, "(UID INTERNALDATE)")

        if type != "OK":
            raise Exception("Could not get message dates")

        messages = []

        for item in data:
            match = self.pattern.search(item.decode("utf-8"))

            if match is None:
                raise Exception("Could not parse message %d data")

            uid = int(match.group(1))
            date = datetime.datetime.strptime(match.group(2), "%d-%b-%Y %H:%M:%S %z")
            delta = self.now - date

            messages.append({
                "uid": uid,
                "date": date,
                "age": delta.days,
                "mailbox": self.build_archive_mailbox(date, mailbox)
            })

        return messages

    def get_mailboxes_matching(self, pattern):
        """Return the mailboxes matching the given pattern"""

        mailbox_names = []
        status, mailboxes = self.connection.list(pattern)

        for mailbox in mailboxes:
            flags, mailbox_name = mailbox.decode().split(' "." ')

            # Mailboxes with the "Noselect" flag cannot be used
            if "Noselect" not in flags:
                mailbox_name = mailbox_name.strip("\"")
                mailbox_names.append(mailbox_name)

        return mailbox_names

if __name__ == "__main__":
    hostname = "localhost"
    username = "username"
    password = "password"

    mailboxes = [
        "INBOX",
        "INBOX.Foo",
        "INBOX.Bar",
        "INBOX.Baz",
        "Sent"
    ]

    try:
        connection = imaplib.IMAP4_SSL(hostname)
        connection.login(username, password)
    except:
        raise Exception("Connection failed")

    archiver = ImapArchiver(connection)

    for mailbox in mailboxes:
        archiver.archive_mailbox(mailbox)
