import exceptions

__all__ = ['ICCError', 'CmdError', 'CommError','PhaseMicroBusy','UndefinedCommand']

class ICCError(exceptions.Exception):
    """ A general exception for the ICC. Anything can throw one, passing a one line error message.
        The top-level event loop will close/cleanup/destroy any running command and return the
        error message on text.
    """

    def __init__(self, error, details=None):
        """ Create an ICCError.

        Args:
           error   - one line of text, intended for users. Will be returned on text.
           details - optional text, intended for operators/programmers. Will be returned on debugText.
        """

        self.error = error
        self.details = details
        if details:
            self.args = (error, details)
        else:
            self.args = (error,)
            
class CmdError(exceptions.Exception):
    """ A exception due to commands sent to the ICC. Anything can throw one, passing a one line
        error message. The top-level event loop will close/cleanup/destroy any running command
        and return the error message on text.
    """

    def __init__(self, error, details=None):
        """ Create a CmdError.

        Args:
           error   - one line of text, intended for users. Will be returned on text.
           details - optional text, intended for operators/programmers. Will be returned on debugText.
        """

        self.error = error
        self.details = details
        if details:
            self.args = (error, details)
        else:
            self.args = (error,)
                 
class CommError(exceptions.Exception):
    """ An exception that specifies that a low-level communication error occurred. These should only
        be thrown for serious communications errors. The top-level event loop will close/cleanup/destroy
        any running command. The error message will be returned on text. 
    """

    def __init__(self, device, error, details=None):
        """ Create a CommError.

        Args:
           device  - name of the device that had an error.
           error   - one line of text, intended for users. Will be returned on text.
           details - optional text, intended for operators/programmers. Will be returned on debugText.
       """

        self.device = device
        self.error = error
        self.details = details
        if details:
            self.args = (device, error, details)
        else:
            self.args = (device, error)
            
