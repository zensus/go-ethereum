package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/codegangsta/cli"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/peterh/liner"
)

const (
	Version = ""
)

func main() {
	app := cli.NewApp()
	app.Name = "gethkey"
	app.Action = gethkey
	app.HideVersion = true // we have a command to print the version
	app.Usage = `
    gethkey [-p <passwordfile>|-d <keydir>] <address> <keyfile>
Exports the given account's private key into <keyfile> using the hex encoding canonical EC
format.
The user is prompted for a passphrase to unlock it.
For non-interactive use, the passphrase can be specified with the --password|-p flag:
    gethkey --password <passwordfile>  <address> <keyfile>
You can set an alternative key directory to use to find your ethereum encrypted keyfile.
Note:
As you can directly copy your encrypted accounts to another ethereum instance,
this import/export mechanism is not needed when you transfer an account between
nodes.
          `
	app.Flags = []cli.Flag{
		keyDirFlag,
		passwordFileFlag,
		lightKDFFlag,
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var passwordFileFlag = cli.StringFlag{
	Name:  "password",
	Usage: "Path to password file (not recommended for interactive use)",
	Value: "",
}
var keyDirFlag = utils.DirectoryFlag{
	Name:  "keydir",
	Usage: "Key directory to be used",
	Value: utils.DirectoryString{path.Join(common.DefaultDataDir(), "keys")},
}
var lightKDFFlag = cli.BoolFlag{
	Name:  "lightkdf",
	Usage: "Reduce KDF memory & CPU usage at some expense of KDF strength",
}

func getPassPhrase(ctx *cli.Context, desc string) (passphrase string) {
	passfile := ctx.GlobalString(passwordFileFlag.Name)
	if len(passfile) == 0 {
		fmt.Println(desc)
		auth, err := readPassword("Passphrase: ", true)
		if err != nil {
			utils.Fatalf("%v", err)
		}
		passphrase = auth
	} else {
		passbytes, err := ioutil.ReadFile(passfile)
		if err != nil {
			utils.Fatalf("Unable to read password file '%s': %v", passfile, err)
		}
		passphrase = string(passbytes)
	}
	return
}

func readPassword(prompt string, warnTerm bool) (string, error) {
	if liner.TerminalSupported() {
		lr := liner.NewLiner()
		defer lr.Close()
		return lr.PasswordPrompt(prompt)
	}
	if warnTerm {
		fmt.Println("!! Unsupported terminal, password will be echoed.")
	}
	fmt.Print(prompt)
	input, err := bufio.NewReader(os.Stdin).ReadString('\n')
	fmt.Println()
	return input, err
}

func gethkey(ctx *cli.Context) {
	account := ctx.Args().First()
	if len(account) == 0 {
		utils.Fatalf("account address must be given as first argument")
	}
	keyfile := ctx.Args().Get(1)
	if len(keyfile) == 0 {
		utils.Fatalf("keyfile must be given as second argument")
	}
	keydir := ctx.GlobalString(keyDirFlag.Name)

	scryptN := crypto.StandardScryptN
	scryptP := crypto.StandardScryptP
	if ctx.GlobalBool(lightKDFFlag.Name) {
		scryptN = crypto.LightScryptN
		scryptP = crypto.LightScryptP
	}
	ks := crypto.NewKeyStorePassphrase(keydir, scryptN, scryptP)
	am := accounts.NewManager(ks)

	err := am.Export(keyfile, common.HexToAddress(account), getPassPhrase(ctx, ""))
	if err != nil {
		utils.Fatalf("Account export failed: %v", err)
	}
}
