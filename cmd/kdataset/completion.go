package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func completionCmd(rootCmd *cobra.Command) *cobra.Command {
	progma := rootCmd.Use
	return &cobra.Command{
		Use:   "bash-completion",
		Short: "Generates bash completion scripts",
		Long: fmt.Sprintf(`To load completion run

. <(%v bash-completion)

To configure your bash shell to load completions for each session add to your bashrc

# ~/.bashrc or ~/.profile
. <(%v bash-completion)
`, progma, progma),
		Run: func(cmd *cobra.Command, args []string) {
			rootCmd.GenBashCompletion(os.Stdout)
		},
	}
}
