import os
import sys
import warnings
from email.utils import parseaddr


class Mail:
    def __init__(self, to, subject):
        self.to = self._getRecipient_(to)
        self.subject = subject
        self.cc = None
        self.bcc = None
        self.message = None
        self.attachment = None
        self.msgType = None

    def ccTo(self, cc):
        self.cc = self._getRecipient_(cc)

    def bccTo(self, bcc):
        self.bcc = self._getRecipient_(bcc)

    def thisMessage(self, message, msgType="text"):
        self.message = message.strip()
        self.msgType = msgType.strip()

    def withAttachment(self, attachment):
        self.attachment = attachment

    def _sanityCheck_(self):
        if self.to is None:
            raise Exception("Recipient Is Empty!!")

        if self.subject is None:
            raise Exception("Subject Is Empty !!")

        if self.message is None:
            warnings.warn("Empty Message!!")

        if (self.msgType.lower() == "html") and (self.attachment is not None):
            warnings.warn("Attachment Not Supported by html messages. Attachment will be ignored")

    def _isValidEmail_(self, emailStr):
        return parseaddr(emailStr)[1] != ""

    def _getRecipient_(self, recipient):
        if type(recipient) == type([]):
            recipList = [self._isValidEmail_(aRecip) and aRecip or "" for aRecip in recipient]
        elif type(recipient) == type(""):
            recipList = [self._isValidEmail_(recipient) and recipient or "", ]
        else:
            recipList = ["", ]
        if set(recipList) == ["", ]:
            return None
        else:
            return ",".join(set(recipList))

    def makeEmailCommand(self):
        ## Make the attachment Command
        if self.msgType.lower() == "text":
            attachmentCmd = []
        elif self.msgType.lower() == "html":
            attachmentCmd = ["-a", '"Content-Type: text/html"']
        elif self.msgType.lower() == "attachement":
            attachmentCmd = ["-a", '"{}"'.format(self.attachment)]
        else:
            attachmentCmd = []

        ## Make the Message Command
        if self.message is not None:
            msgCmd = ["<", self.message]
        else:
            msgCmd = ["<<", ""]

        mailCommand = ["mailx", "-v"] + attachmentCmd + ["-s", '"{}"'.format(self.subject)]

        if self.cc is not None:
            mailCommand = mailCommand + ["-c", '"{}"'.format(self.cc)]

        if self.bcc is not None:
            mailCommand = mailCommand + ["-b", '"{}"', format(self.bcc)]

        mailCommand.append('"{}"'.format(self.to))
        mailCommand = mailCommand + msgCmd

        return " ".join(mailCommand)

    def executeCommand(self, cmd):
        sys.stdout.flush()
        rc = os.system(cmd)
        if rc > 0:
            raise Exception('Mail Sending Failed')

    def send(self):
        self._sanityCheck_()
        mailCommand = self.makeEmailCommand()
        print(mailCommand)
        self.executeCommand(mailCommand)
