
import sys
import importlib
from typing import Any
from .inspect_wrapped import inspect_wrapped
from .command import Command, create_command
from .group import Group, create_group


class CLI:
    entrypoint = Group(
        None,
        None,
        ""
    )
    

    @classmethod
    async def run(
        cls,
        args: list[str] | None = None
    ):
        if args is None:
            args  = list(sys.argv)

        await cls.entrypoint.run(args)

    @classmethod
    def root(
        cls,
        *commands: list[Group | Command],
        shortnames: dict[str, str] | None = None,
    ):

        if shortnames is None:
            shortnames = {}

        def wrap(command_call): 

            (
                positional_args_map, 
                keyword_args_map, 
                help_message,
            ) = inspect_wrapped(
                command_call,
                shortnames=shortnames,
                indentation=3,
            )
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
            cls.entrypoint.update_command(
                command_call.__name__,
                command_call,
                help_message,
                positional_args=positional_args_map,
                keyword_args_map=keyword_args_map,
            )

            for command in commands:
                if isinstance(command, Group):
                    cls.entrypoint.subgroups[command.group_name] = command

                elif isinstance(command, Command):
                    cls.entrypoint.subcommands[command.command_name] = command
                
            return cls

        return wrap
   
    @classmethod 
    def group(
        cls,
        shortnames: dict[str, str] | None = None,
    ):

        if shortnames is None:
            shortnames = {}
            
        def wrap(command):  
            if cls.entrypoint.source:
                cls.entrypoint.group(shortnames=shortnames)(command)

            else:
                return create_group(
                    command,
                    shortnames=shortnames,
                )

        return wrap
    
    @classmethod
    def command(
        cls,
        shortnames: dict[str, str] | None = None,
    ):
        if shortnames is None:
            shortnames = {}

        def wrap(command):
            if cls.entrypoint.source:
                cls.entrypoint.command(shortnames=shortnames)(command)

            return create_command(
                command,
                shortnames=shortnames,
            )
        
        return wrap
    


   