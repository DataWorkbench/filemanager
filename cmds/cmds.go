package cmds

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/DataWorkbench/common/utils/buildinfo"
	"github.com/DataWorkbench/filemanager/config"
	"github.com/DataWorkbench/filemanager/server"
)

var (
	versionFlag bool
)

var root = &cobra.Command{
	Use:   "fileManager",
	Short: "DataWorkbench File Manager",
	Long:  "DataWorkbench File Manager",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		if versionFlag {
			fmt.Println(buildinfo.MultiString)
			return
		}
		_ = cmd.Help()
	},
}

var start = &cobra.Command{
	Use:   "start",
	Short: "Command to start server",
	Long:  "Command to start server",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		if err := server.Start(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "start server fail: %v\n", err)
			os.Exit(1)
		}
	},
}

func Execute(){
	root.AddCommand(start)

	if err := root.Execute();err!=nil{
		os.Exit(1)
	}
}

func init() {
	// set root command flags
	root.Flags().BoolVarP(
		&versionFlag, "version", "v", false, "show the version",
	)

	// set start command flags
	start.Flags().StringVarP(
		&config.FilePath, "config", "c", "", "path of config file",
	)
}